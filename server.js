const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const dotenv = require('dotenv');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const compression = require('compression');

// Load environment variables
dotenv.config();

// Resource limiting constants
const LIMITS = {
  MAX_ROOMS: parseInt(process.env.MAX_ROOMS) || 10,
  MAX_USERS_PER_ROOM: parseInt(process.env.MAX_USERS_PER_ROOM) || 4,
  MAX_MESSAGE_SIZE: parseInt(process.env.MAX_MESSAGE_SIZE) || 64 * 1024, // 64KB
  MAX_PING_INTERVAL: parseInt(process.env.MAX_PING_INTERVAL) || 30000, // 30s
  MAX_INACTIVE_THRESHOLD: parseInt(process.env.MAX_INACTIVE_THRESHOLD) || 120000 // 2m
};

// Add this near other constants
const HOST_COMMAND_LIMITS = {
  CONFIRMATION_REQUIRED: ['remove-all', 'remove-user'],
  RATE_LIMITS: {
    'mute-all': { count: 3, period: 60000 }, // 3 per minute
    'remove-user': { count: 5, period: 60000 }, // 5 per minute
    'remove-all': { count: 1, period: 300000 }, // 1 per 5 minutes
  }
};

// Add these constants near other app constants
const CONNECTION_HEALTH = {
  MAX_MISSED_PINGS: 3,               // Consider disconnected after this many consecutive missed pings
  HEALTH_WINDOW_SIZE: 10,            // Track this many recent ping-pongs for health metrics
  PING_TIMEOUT: 15000,               // Maximum time to wait for a pong response (15 seconds)
  RELIABILITY_THRESHOLD_POOR: 0.5,   // Below 50% response rate is poor
  RELIABILITY_THRESHOLD_FAIR: 0.8,   // Below 80% response rate is fair
  RELIABILITY_THRESHOLD_GOOD: 0.95   // Below 95% response rate is good, above is excellent
};

// Initialize Express app and HTTP server
const app = express();
const server = http.createServer(app);

// LRU Cache implementation for room data
class LRUCache {
  constructor(maxSize = 1000) {
    this.maxSize = maxSize;
    this.cache = new Map();
    this.keys = [];
    this.timeouts = new Map(); // Track timeouts for TTL-based expiration
  }
  
  get(key) {
    if (!this.cache.has(key)) return null;
    
    // Move to most recently used
    this.keys = this.keys.filter(k => k !== key);
    this.keys.push(key);
    
    return this.cache.get(key);
  }
  
  set(key, value, ttl = 0) {
    // Clear any existing timeout to prevent memory leaks and early eviction
    this._clearTimeout(key);
    
    // Update existing or add new
    if (this.cache.has(key)) {
      this.keys = this.keys.filter(k => k !== key);
    } else if (this.keys.length >= this.maxSize) {
      // Evict least recently used
      const lru = this.keys.shift();
      this._clearTimeout(lru); // Clear timeout for evicted entry
      this.cache.delete(lru);
    }
    
    this.keys.push(key);
    this.cache.set(key, value);
    
    // Optional TTL
    if (ttl > 0) {
      const timeoutId = setTimeout(() => {
        if (this.cache.has(key)) {
          this.delete(key);
        }
      }, ttl);
      
      // Store the timeout ID for potential early cancellation
      this.timeouts.set(key, timeoutId);
    }
  }
  
  delete(key) {
    this._clearTimeout(key);
    this.cache.delete(key);
    this.keys = this.keys.filter(k => k !== key);
  }
  
  // Helper method to clear timeouts
  _clearTimeout(key) {
    if (this.timeouts.has(key)) {
      clearTimeout(this.timeouts.get(key));
      this.timeouts.delete(key);
    }
  }
  
  // Additional utility methods
  
  // Get the number of entries in the cache
  size() {
    return this.cache.size;
  }
  
  // Check if a key exists
  has(key) {
    return this.cache.has(key);
  }
  
  // Clear the entire cache
  clear() {
    // Clear all timeouts first
    for (const timeoutId of this.timeouts.values()) {
      clearTimeout(timeoutId);
    }
    
    this.cache.clear();
    this.keys = [];
    this.timeouts.clear();
  }
  
  // Optional: refresh TTL for an existing entry
  refreshTTL(key, ttl) {
    if (!this.cache.has(key)) return false;
    
    const value = this.cache.get(key);
    this.set(key, value, ttl);
    return true;
  }
}

// Priority queue for room cleanup
class RoomPriorityQueue {
  constructor() {
    this.rooms = [];
  }
  
  add(roomId, timestamp) {
    // Find and update if exists
    const index = this.rooms.findIndex(r => r.roomId === roomId);
    if (index >= 0) {
      this.rooms[index].timestamp = timestamp;
    } else {
      this.rooms.push({ roomId, timestamp });
    }
    // Sort by oldest first (ascending timestamp)
    this.rooms.sort((a, b) => a.timestamp - b.timestamp);
  }
  
  getOldestRooms(threshold) {
    const now = Date.now();
    return this.rooms.filter(r => now - r.timestamp > threshold);
  }
  
  remove(roomId) {
    this.rooms = this.rooms.filter(r => r.roomId !== roomId);
  }
}

// Server metrics tracking
const serverMetrics = {
  startTime: Date.now(),
  connections: {
    total: 0,
    active: 0
  },
  messages: {
    total: 0,
    byType: {}
  },
  rooms: {
    total: 0,
    active: 0
  },
  errors: {
    total: 0,
    byType: {}
  },
  network: {
    avgLatency: 0,
    avgJitter: 0,
    pingStats: {
      sent: 0,
      received: 0, 
      lost: 0
    },
    latencyDistribution: {
      excellent: 0,    // 0-50ms
      good: 0,         // 50-100ms
      fair: 0,         // 100-200ms
      poor: 0          // >200ms
    }
  },
  
  recordConnection() {
    this.connections.total++;
    this.connections.active++;
  },
  
  recordDisconnection() {
    this.connections.active--;
  },
  
  recordMessage(type) {
    this.messages.total++;
    this.messages.byType[type] = (this.messages.byType[type] || 0) + 1;
  },
  
  recordError(type) {
    this.errors.total++;
    this.errors.byType[type] = (this.errors.byType[type] || 0) + 1;
  },

  recordNetworkStats() {
    // Collect network statistics from all active connections
    const connections = [];
    let totalLatency = 0;
    let totalJitter = 0;
    let count = 0;
    
    // Reset latency distribution
    this.network.latencyDistribution = {
      excellent: 0,
      good: 0,
      fair: 0,
      poor: 0
    };

    wss.clients.forEach(client => {
      if (client.connectionQuality && client.isAlive) {
        connections.push(client.connectionQuality);
        
        if (client.connectionQuality.avgLatency) {
          totalLatency += client.connectionQuality.avgLatency;
          
          // Update latency distribution
          const latency = client.connectionQuality.avgLatency;
          if (latency <= 50) {
            this.network.latencyDistribution.excellent++;
          } else if (latency <= 100) {
            this.network.latencyDistribution.good++;
          } else if (latency <= 200) {
            this.network.latencyDistribution.fair++;
          } else {
            this.network.latencyDistribution.poor++;
          }
        }
        
        if (client.connectionQuality.jitter) {
          totalJitter += client.connectionQuality.jitter;
        }
        
        count++;
      }
    });
    
    // Update averages
    if (count > 0) {
      this.network.avgLatency = Math.round(totalLatency / count);
      this.network.avgJitter = Math.round(totalJitter / count);
    }
    
    return connections;
  },
  
  getStats() {
    // Update network stats before returning
    const networkConnections = this.recordNetworkStats();
    
    return {
      uptime: Math.floor((Date.now() - this.startTime) / 1000),
      connections: this.connections,
      messages: this.messages,
      rooms: {
        total: rooms.size,
        active: Array.from(rooms.values()).filter(room => room.size > 0).length
      },
      errors: this.errors,
      network: {
        ...this.network,
        connections: networkConnections.length
      },
      memory: {
        rss: Math.round(process.memoryUsage().rss / (1024 * 1024)),
        heapTotal: Math.round(process.memoryUsage().heapTotal / (1024 * 1024)),
        heapUsed: Math.round(process.memoryUsage().heapUsed / (1024 * 1024))
      }
    };
  }
};

// Pre-compile frequent message templates
const messageTemplates = {
  roomJoined: (roomId, participants, isHost) => (
    JSON.stringify({
      type: 'room-joined',
      roomId,
      participants,
      isHost
    })
  ),
  roomRejoined: (roomId, participants, isHost) => (
    JSON.stringify({
      type: 'room-rejoined',
      roomId,
      participants,
      isHost
    })
  ),
  userJoined: (userId, userName, isHost) => (
    JSON.stringify({
      type: 'user-joined',
      userId,
      userName,
      isHost
    })
  ),
  userLeft: (userId, userName) => (
    JSON.stringify({
      type: 'user-left',
      userId,
      userName
    })
  ),
  userDisconnected: (userId, userName, temporary, wasHost) => (
    JSON.stringify({
      type: 'user-disconnected',
      userId,
      userName,
      temporary,
      wasHost
    })
  ),
  userReconnected: (userId, userName, wasHost, audioEnabled, videoEnabled) => (
    JSON.stringify({
      type: 'user-reconnected',
      userId,
      userName,
      wasHost,
      audioEnabled,
      videoEnabled
    })
  ),
  roomClosed: (roomId, hostId, hostName, reason) => (
    JSON.stringify({
      type: 'room-closed',
      roomId,
      hostId,
      hostName,
      reason: reason || 'Room closed by host'
    })
  ),
  hostChanged: (oldHostId, newHostId, newHostName, permanent) => (
    JSON.stringify({
      type: 'host-changed',
      oldHostId,
      newHostId,
      newHostName,
      permanent: !!permanent
    })
  ),
  leaveConfirmed: () => (
    JSON.stringify({
      type: 'leave-confirmed'
    })
  ),
  error: (code, message) => (
    JSON.stringify({
      type: 'error',
      error: code,
      message
    })
  )
};

// Enable CORS and use compression for all responses
app.use(cors());
app.use(compression({
  level: 6, // Balanced compression level
  threshold: 1024 // Only compress responses larger than 1KB
}));

// Serve static files with proper caching
const ONE_DAY = 86400000;
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: ONE_DAY,
  etag: true,
  lastModified: true
}));

// Add a route for the main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Add a route for joining a specific room
app.get('/join/:roomId', (req, res) => {
  res.redirect(`/?room=${req.params.roomId}`);
});

// Route for health check
app.get('/health', (req, res) => {
  const stats = serverMetrics.getStats();
  res.status(200).json({
    status: 'OK',
    uptime: stats.uptime,
    connections: stats.connections.active,
    rooms: stats.rooms.total,
    network: {
      avgLatency: stats.network.avgLatency,
      avgJitter: stats.network.avgJitter,
      connectionQuality: getNetworkQualityRating(stats.network.avgLatency, stats.network.avgJitter)
    }
  });
});

// Add metrics endpoint with enhanced network statistics
app.get('/metrics', (req, res) => {
  const stats = serverMetrics.getStats();
  
  // Add network quality score calculation
  if (stats.network) {
    stats.network.qualityScore = calculateOverallNetworkScore(stats.network);
    stats.network.qualityRating = getNetworkQualityRating(
      stats.network.avgLatency, 
      stats.network.avgJitter
    );
  }
  
  res.json(stats);
});

// Helper function to get a qualitative network rating
function getNetworkQualityRating(latency, jitter) {
  if (!latency && !jitter) return 'Unknown';
  
  // Create a score based on latency and jitter
  const latencyScore = latency ? Math.max(0, 1 - (latency / 300)) : 0.5; // 300ms is very poor
  const jitterScore = jitter ? Math.max(0, 1 - (jitter / 100)) : 0.5;    // 100ms jitter is very poor
  
  // Combined score (0-1)
  const combinedScore = (latencyScore * 0.7) + (jitterScore * 0.3);
  
  // Convert to rating
  if (combinedScore >= 0.8) return 'Excellent';
  if (combinedScore >= 0.6) return 'Good';
  if (combinedScore >= 0.4) return 'Fair';
  if (combinedScore >= 0.2) return 'Poor';
  return 'Very Poor';
}

// Helper function to calculate an overall network score
function calculateOverallNetworkScore(networkStats) {
  if (!networkStats) return 0;
  
  const latency = networkStats.avgLatency || 0;
  const jitter = networkStats.avgJitter || 0;
  
  // Normalize metrics to 0-1 scale (0 = bad, 1 = good)
  const normalizedLatency = Math.max(0, 1 - (latency / 300)); // 300ms is considered very poor
  const normalizedJitter = Math.max(0, 1 - (jitter / 100));   // 100ms jitter is very poor
  
  // Calculate connection distribution score
  let distributionScore = 0;
  const distribution = networkStats.latencyDistribution;
  if (distribution) {
    const total = distribution.excellent + distribution.good + distribution.fair + distribution.poor;
    if (total > 0) {
      distributionScore = (
        (distribution.excellent * 1.0) + 
        (distribution.good * 0.7) + 
        (distribution.fair * 0.4) + 
        (distribution.poor * 0.1)
      ) / total;
    }
  }
  
  // Weightings
  const weights = {
    latency: 0.4,
    jitter: 0.3,
    distribution: 0.3
  };
  
  // Calculate weighted score (0-100)
  const score = 100 * (
    (weights.latency * normalizedLatency) +
    (weights.jitter * normalizedJitter) +
    (weights.distribution * distributionScore)
  );
  
  return Math.round(score);
}

// Optimized WebSocket server configuration
const wss = new WebSocket.Server({ 
  server,
  perMessageDeflate: {
    zlibDeflateOptions: {
      chunkSize: 1024,
      memLevel: 9,  // Increased from 7
      level: 2      // Decreased from 3 for faster compression
    },
    zlibInflateOptions: {
      chunkSize: 16 * 1024  // Increased from 10KB to 16KB
    },
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    concurrencyLimit: 20,   // Increased from 10
    threshold: 512          // Lowered from 1024 bytes
  },
  maxPayload: LIMITS.MAX_MESSAGE_SIZE
});

// Initialize data structures
const rooms = new Map();
const userSessions = new Map();
const roomCache = new LRUCache(500); // Limit to 500 entries
const roomQueue = new RoomPriorityQueue();
const roomActivity = new Map();

// Constants for room management
const PING_INTERVAL = LIMITS.MAX_PING_INTERVAL;
const CONNECTION_TIMEOUT = 10000; // 10 seconds
const ROOM_CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutes
const ROOM_INACTIVE_THRESHOLD = LIMITS.MAX_INACTIVE_THRESHOLD;

// Add this to track command usage
const hostCommandTracker = new Map();

// Helper function to check rate limits
function checkCommandRateLimit(userId, command) {
  const now = Date.now();
  const limit = HOST_COMMAND_LIMITS.RATE_LIMITS[command];
  
  if (!limit) return true; // No limit for this command
  
  const key = `${userId}:${command}`;
  if (!hostCommandTracker.has(key)) {
    hostCommandTracker.set(key, { count: 1, timestamps: [now] });
    return true;
  }
  
  const tracker = hostCommandTracker.get(key);
  
  // Remove timestamps older than the period
  tracker.timestamps = tracker.timestamps.filter(t => now - t < limit.period);
  
  // Check if under the limit
  if (tracker.timestamps.length < limit.count) {
    tracker.timestamps.push(now);
    tracker.count++;
    return true;
  }
  
  return false;
}

// Update room activity function
function updateRoomActivity(roomId) {
  const timestamp = Date.now();
  roomActivity.set(roomId, timestamp);
  roomQueue.add(roomId, timestamp);
}

// Function to remove user from room
function removeUserFromRoom(userId, roomId) {
  const room = rooms.get(roomId);
  if (room) {
    // Get user info before removing
    const user = room.get(userId);
    const userName = user ? user.userName : null;
    const isHost = user ? user.isHost : false;
    
    // Remove user from room
    room.delete(userId);
    
    // Remove from user sessions
    userSessions.delete(userId);
    
    // Clear any cached data for this user or room
    roomCache.delete(`user-${userId}`);
    roomCache.delete(`room-${roomId}`);
    
    // Validate room after user removal
    const isRoomValid = validateRoom(roomId);
    
    if (isRoomValid) {
      // Only broadcast if room still exists and has users
      broadcastToRoom(roomId, JSON.parse(messageTemplates.userLeft(userId, userName)));
      
      // Handle host transfer if needed
      if (isHost && room.size > 0) {
        const newHostCandidate = findNewHostCandidate(room, userId);
        if (newHostCandidate) {
          const [newHostId, newHost] = [newHostCandidate, room.get(newHostCandidate)];
          transferHostRole(roomId, userId, newHostId);
          
          broadcastToRoom(roomId, JSON.parse(messageTemplates.hostChanged(
            userId, newHostId, newHost.userName, true // permanent transfer
          )));
        }
      }
    }
  }
}

// Improved broadcast function with efficient message delivery and proper cleanup
function broadcastToRoom(roomId, message, excludeUserId = null) {
  const room = rooms.get(roomId);
  if (!room) return 0;
  
  // Update room activity
  updateRoomActivity(roomId);
  
  // Create excluded set for faster lookups
  const excluded = new Set(excludeUserId ? [excludeUserId] : []);
  
  // Convert message to JSON once if not already a string
  let jsonMessage;
  try {
    jsonMessage = typeof message === 'string' ? message : JSON.stringify(message);
  } catch (error) {
    console.error('Failed to stringify message in broadcastToRoom:', error);
    // Create a safe fallback message
    jsonMessage = JSON.stringify({
      type: 'error',
      error: 'internal-error',
      message: 'Failed to process message data'
    });
    serverMetrics.recordError('json-stringify-failed');
  }
  
  if (!jsonMessage) {
    console.error('Invalid message provided to broadcastToRoom');
    return 0;
  }
  
  let sentCount = 0;
  let disconnectedUsers = [];
  
  // Send to all connected users not in excluded set
  for (const [userId, user] of room.entries()) {
    if (excluded.has(userId)) continue;
    
    // Skip already known disconnected users for efficiency
    if (!user.connected || user.ws.readyState !== WebSocket.OPEN) {
      disconnectedUsers.push(userId);
      continue;
    }
    
    try {
      user.ws.send(jsonMessage);
      sentCount++;
    } catch (error) {
      console.error(`Error sending to ${userId}:`, error.message);
      disconnectedUsers.push(userId);
    }
  }
  
  // Handle disconnected users after the broadcast loop
  if (disconnectedUsers.length > 0) {
    // Clean up disconnected users from the room
    for (const userId of disconnectedUsers) {
      const user = room.get(userId);
      if (user) {
        // If already marked as disconnected, leave it to the timeout to clean up
        if (!user.connected) continue;
        
        // Mark as disconnected and start timeout for potential reconnect
        user.connected = false;
        user.disconnectedAt = Date.now();
        
        // Schedule immediate disconnect handling if not already done
        process.nextTick(() => {
          // Check if this user is still in the room - they might have been
          // removed already by another process
          if (room.has(userId)) {
            handleDisconnection({ userId, roomId });
          }
        });
      }
    }
  }
  
  return sentCount;
}

// Improved host transfer mechanism with complete state updates
function transferHostRole(roomId, oldHostId, newHostId) {
  const room = rooms.get(roomId);
  if (!room) {
    console.error(`Cannot transfer host: Room ${roomId} not found`);
    return false;
  }

  const oldHost = room.get(oldHostId);
  const newHost = room.get(newHostId);
  
  if (!newHost) {
    console.error(`Cannot transfer host: New host ${newHostId} not found in room ${roomId}`);
    return false;
  }

  // Update host status in room data
  if (oldHost) {
    oldHost.isHost = false;
    console.log(`Host status removed from ${oldHostId} in room ${roomId}`);
  }
  
  newHost.isHost = true;
  console.log(`Host status granted to ${newHostId} in room ${roomId}`);

  // Update session data - critical for persistence across reconnects
  const oldHostSession = userSessions.get(oldHostId);
  const newHostSession = userSessions.get(newHostId);
  
  if (oldHostSession) {
    oldHostSession.isHost = false;
  }
  
  if (newHostSession) {
    newHostSession.isHost = true;
  } else {
    // Create a session if it doesn't exist
    userSessions.set(newHostId, {
      roomId: roomId,
      userName: newHost.userName,
      isHost: true
    });
    console.log(`Created new session for host ${newHostId} in room ${roomId}`);
  }

  // If the newHost's WebSocket exists, make sure the user knows they're now host
  if (newHost.connected && newHost.ws && newHost.ws.readyState === WebSocket.OPEN) {
    try {
      newHost.ws.send(JSON.stringify({
        type: 'host-status-changed',
        isHost: true,
        message: 'You are now the host of this room'
      }));
      console.log(`Notified ${newHostId} of new host status`);
    } catch (error) {
      console.error(`Failed to notify new host ${newHostId}:`, error.message);
    }
  }

  return true;
}

// Fix for findNewHostCandidate to ensure we select a connected user
function findNewHostCandidate(room, excludeUserId) {
  // Sort users by connection time (oldest first) and connection status
  const candidates = Array.from(room.entries())
    .filter(([userId, user]) => userId !== excludeUserId)
    .filter(([_, user]) => user.connected && user.ws && user.ws.readyState === WebSocket.OPEN) // Only consider connected users
    .sort((a, b) => a[1].joinedAt - b[1].joinedAt); // Oldest joined first

  if (candidates.length > 0) {
    console.log(`Selected new host candidate: ${candidates[0][0]}`);
    return candidates[0][0];
  }
  
  console.log(`No suitable host candidates found in room`);
  return null;
}

// Function to validate and cleanup rooms
function validateRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return false;
  
  // Remove any users with closed connections
  for (const [userId, user] of room.entries()) {
    if (!user.connected || user.ws.readyState !== WebSocket.OPEN) {
      console.log(`Removing disconnected user ${userId} from room ${roomId}`);
      room.delete(userId);
      userSessions.delete(userId);
    }
  }
  
  // If room is empty after cleanup, delete it
  if (room.size === 0) {
    console.log(`Deleting empty room ${roomId} after validation`);
    rooms.delete(roomId);
    roomActivity.delete(roomId);
    roomQueue.remove(roomId);
    return false;
  }
  
  return true;
}

// Optimized error sending function
function sendError(ws, code, message) {
  const errorKey = `error-${code}-${message}`;
  let errorJson = roomCache.get(errorKey);
  
  if (!errorJson) {
    errorJson = messageTemplates.error(code, message);
    roomCache.set(errorKey, errorJson, 10 * 60 * 1000); // Cache for 10 mins
  }
  
  try {
    ws.send(errorJson);
    serverMetrics.recordError(code);
  } catch (error) {
    console.error('Error sending error message:', error.message);
  }
}

// Enhanced ping system with improved connection tracking
const pingAllClients = () => {
  const currentTime = Date.now();
  let activePings = 0;
  let closedConnections = 0;
  
  wss.clients.forEach((ws) => {
    // Initialize connection tracking data if not exists
    if (!ws.pingStats) {
      ws.pingStats = {
        missedPings: 0,
        totalPings: 0,
        successfulPongs: 0,
        pingHistory: [],  // Array of {sent, received, latency}
        lastPingTime: 0,
        reliability: 1.0
      };
    }
    
    // Check if previous ping timed out
    if (ws.lastPingTime && currentTime - ws.lastPingTime > CONNECTION_HEALTH.PING_TIMEOUT) {
      ws.pingStats.missedPings++;
      
      // Add a missed ping to history
      if (ws.pingStats.pingHistory.length >= CONNECTION_HEALTH.HEALTH_WINDOW_SIZE) {
        ws.pingStats.pingHistory.shift(); // Remove oldest entry
      }
      
      ws.pingStats.pingHistory.push({
        sent: ws.lastPingTime,
        received: null,
        latency: null,
        missed: true
      });
      
      // Update reliability metric (% of successful pongs in window)
      const recentHistory = ws.pingStats.pingHistory;
      const successfulPongs = recentHistory.filter(ping => !ping.missed).length;
      ws.pingStats.reliability = recentHistory.length > 0 ? 
        successfulPongs / recentHistory.length : 1.0;
      
      console.log(`Client ${ws.userId || 'unknown'} missed ping, reliability: ${ws.pingStats.reliability.toFixed(2)}`);
    }
    
    // Check if connection is considered dead (too many consecutive missed pings)
    if (ws.pingStats.missedPings >= CONNECTION_HEALTH.MAX_MISSED_PINGS) {
      closedConnections++;
      console.log(`Terminating connection for ${ws.userId || 'unknown'} after ${ws.pingStats.missedPings} consecutive missed pings`);
      
      // Record disconnection event
      if (ws.pingHistory) {
      ws.pingHistory.push({
          status: 'disconnected-timeout',
          timestamp: currentTime,
          reliability: ws.pingStats.reliability
      });
      }
      
      handleDisconnection(ws);
      ws.terminate();
      return;
    }
    
    // Send new ping
    ws.lastPingTime = currentTime;
    ws.pingStats.totalPings++;
    ws.pingStats.missedPings = 0; // Reset consecutive missed pings counter when sending new ping
    
    try {
      // Send WebSocket protocol ping
      ws.ping();
      
      // Also send application-level ping to measure app-level latency
      if (ws.readyState === WebSocket.OPEN) {
        const pingData = JSON.stringify({ type: 'ping', timestamp: currentTime });
        ws.send(pingData);
      }
      
      activePings++;
      
      // Add ping attempt to limited history
      if (ws.pingHistory) {
      ws.pingHistory.push({
        status: 'ping-sent',
        timestamp: currentTime
      });
      
      if (ws.pingHistory.length > 50) {
        ws.pingHistory.shift();
        }
      }
    } catch (error) {
      console.error(`Error sending ping to client ${ws.userId || 'unknown'}:`, error.message);
      ws.pingStats.missedPings++;
    }
  });
  
  // Only log if there's activity to reduce noise
  if (closedConnections > 0) {
    console.log(`Ping cycle: ${activePings} active, ${closedConnections} terminated`);
  }
};

// Enhanced pong handler with improved connection tracking
const handlePong = function(ws, timestamp) {
  const pongTime = Date.now();
  
  if (!ws.pingStats) {
    ws.pingStats = {
      missedPings: 0,
      totalPings: 0,
      successfulPongs: 0,
      pingHistory: [],
      lastPingTime: 0,
      reliability: 1.0
    };
  }
  
  // Calculate latency
  const pingLatency = ws.lastPingTime ? pongTime - ws.lastPingTime : null;
  
  // Record successful pong
  ws.pingStats.missedPings = 0; // Reset on successful pong
  ws.pingStats.successfulPongs++;
  
  // Add to ping history
  if (ws.pingStats.pingHistory.length >= CONNECTION_HEALTH.HEALTH_WINDOW_SIZE) {
    ws.pingStats.pingHistory.shift(); // Remove oldest entry
  }
  
  ws.pingStats.pingHistory.push({
    sent: ws.lastPingTime,
    received: pongTime,
    latency: pingLatency,
    missed: false
  });
  
  // Update connection quality metrics
  
  // Calculate average latency
  const validLatencies = ws.pingStats.pingHistory
    .filter(ping => ping.latency !== null)
    .map(ping => ping.latency);
  
  const avgLatency = validLatencies.length > 0 ? 
    validLatencies.reduce((sum, latency) => sum + latency, 0) / validLatencies.length : null;
  
  // Calculate jitter (standard deviation of latencies)
  let jitter = null;
  if (validLatencies.length > 1) {
    const mean = avgLatency;
    const squaredDiffs = validLatencies.map(l => Math.pow(l - mean, 2));
    const variance = squaredDiffs.reduce((a, b) => a + b, 0) / validLatencies.length;
    jitter = Math.round(Math.sqrt(variance));
  }
  
  // Update reliability metric (% of successful pongs in window)
  const recentHistory = ws.pingStats.pingHistory;
  const successfulPongs = recentHistory.filter(ping => !ping.missed).length;
  ws.pingStats.reliability = recentHistory.length > 0 ? 
    successfulPongs / recentHistory.length : 1.0;
  
  // Update connection quality data
  if (!ws.connectionQuality) {
    ws.connectionQuality = {
      avgLatency: 0,
      jitter: 0,
      reliability: 100,
      lastUpdated: Date.now(),
      connectionRating: 'Unknown'
    };
  }
  
  ws.connectionQuality.avgLatency = avgLatency ? Math.round(avgLatency) : ws.connectionQuality.avgLatency;
  ws.connectionQuality.jitter = jitter !== null ? jitter : ws.connectionQuality.jitter;
  ws.connectionQuality.reliability = Math.round(ws.pingStats.reliability * 100);
  ws.connectionQuality.lastUpdated = pongTime;
  
  // Calculate connection rating
  let connectionRating;
  if (ws.pingStats.reliability < CONNECTION_HEALTH.RELIABILITY_THRESHOLD_POOR) {
    connectionRating = 'Poor';
  } else if (ws.pingStats.reliability < CONNECTION_HEALTH.RELIABILITY_THRESHOLD_FAIR) {
    connectionRating = 'Fair';
  } else if (ws.pingStats.reliability < CONNECTION_HEALTH.RELIABILITY_THRESHOLD_GOOD) {
    connectionRating = 'Good';
  } else {
    connectionRating = 'Excellent';
  }
  
  ws.connectionQuality.connectionRating = connectionRating;
  
  // Add to history for debugging
  if (ws.pingHistory) {
    ws.pingHistory.push({
      status: 'pong-received',
      timestamp: pongTime,
      latency: pingLatency,
      reliability: ws.pingStats.reliability
    });
    
    if (ws.pingHistory.length > 50) {
      ws.pingHistory.shift();
    }
  }
};

// Handle disconnection with improved timeout management
function handleDisconnection(ws) {
  const { userId, roomId } = ws;
  if (!roomId || !userId) return;

  const room = rooms.get(roomId);
  if (!room) return;

  const user = room.get(userId);
  if (!user) return;

  const wasHost = user.isHost;
  user.connected = false;
  user.disconnectedAt = Date.now();

  // Notify others about temporary disconnection
  broadcastToRoom(
    roomId, 
    messageTemplates.userDisconnected(userId, user.userName, true, wasHost),
    userId
  );

  // If the disconnected user was the host, immediately try to find a new host
  if (wasHost) {
    const newHostId = findNewHostCandidate(room, userId);
    if (newHostId) {
      if (transferHostRole(roomId, userId, newHostId)) {
        const newHost = room.get(newHostId);
        broadcastToRoom(
          roomId,
          messageTemplates.hostChanged(userId, newHostId, newHost.userName, true)
        );
        console.log(`Host role transferred from ${userId} to ${newHostId} in room ${roomId}`);
      }
    }
  }

  // Set a shorter timeout for host reconnection
  const timeoutDuration = wasHost ? 15000 : 30000; // 15 seconds for host, 30 for others

  // Clear any existing timeout to prevent memory leaks
  if (user.disconnectTimeout) {
    clearTimeout(user.disconnectTimeout);
    user.disconnectTimeout = null;
  }
  
  // Store the timeout so we can clear it if user reconnects
  user.disconnectTimeout = setTimeout(() => {
    const currentRoom = rooms.get(roomId);
    if (!currentRoom) return;

    const currentUser = currentRoom.get(userId);
    if (currentUser && !currentUser.connected) {
      // User didn't reconnect, remove permanently
      removeUserFromRoom(userId, roomId);
      
      // If this was the host and no transfer happened yet, try again
      if (wasHost) {
        const newHostId = findNewHostCandidate(currentRoom, userId);
        if (newHostId) {
          if (transferHostRole(roomId, userId, newHostId)) {
            const newHost = currentRoom.get(newHostId);
            broadcastToRoom(
              roomId, 
              messageTemplates.hostChanged(userId, newHostId, newHost.userName, true)
            );
          }
        } else {
          // No suitable host found, close the room
          broadcastToRoom(
            roomId,
            messageTemplates.roomClosed(
              roomId, userId, user.userName,
              'No active participants to transfer host role'
            )
          );
          rooms.delete(roomId);
          roomQueue.remove(roomId);
        }
      }
    }
  }, timeoutDuration);
}

// Improved room cleanup interval
const roomCleanupInterval = setInterval(() => {
  // Only check rooms that might be inactive
  const inactiveRooms = roomQueue.getOldestRooms(ROOM_INACTIVE_THRESHOLD);
  
  if (inactiveRooms.length === 0) return;
  
  console.log(`Checking ${inactiveRooms.length} potentially inactive rooms`);
  let cleanedRooms = 0;
  
  inactiveRooms.forEach(({ roomId }) => {
    const room = rooms.get(roomId);
    if (!room) {
      roomQueue.remove(roomId);
      return;
    }
    
    // Check if room has active users
    const hasActiveUsers = Array.from(room.values()).some(user => 
      user.connected && user.ws.readyState === WebSocket.OPEN
    );
    
    if (!hasActiveUsers) {
      // Clean up the room
      console.log(`Cleaning up inactive room: ${roomId}`);
      broadcastToRoom(
        roomId,
        messageTemplates.roomClosed(
          roomId, null, null, 'Room closed due to inactivity'
        )
      );
      
      // Cleanup user sessions
      room.forEach((_, userId) => userSessions.delete(userId));
      
      // Remove room
      rooms.delete(roomId);
      roomActivity.delete(roomId);
      roomQueue.remove(roomId);
      cleanedRooms++;
    }
  });
  
  if (cleanedRooms > 0) {
    console.log(`Cleaned up ${cleanedRooms} inactive rooms`);
  }
}, ROOM_CLEANUP_INTERVAL);

// Start the ping interval
const pingIntervalId = setInterval(pingAllClients, PING_INTERVAL);

// WebSocket connection handler
wss.on('connection', (ws) => {
  // Initialize connection state
  ws.lastPingTime = null;
  ws.userId = null;
  ws.roomId = null;
  ws.pingHistory = [];
  ws.pingStats = {
    missedPings: 0,
    totalPings: 0,
    successfulPongs: 0,
    pingHistory: [],
    lastPingTime: 0,
    reliability: 1.0
  };
  ws.connectionQuality = {
    avgLatency: 0,
    jitter: 0,
    reliability: 100,
    lastUpdated: Date.now(),
    connectionRating: 'Unknown'
  };

  // Update metrics
  serverMetrics.recordConnection();
  
  // Enhanced pong handler with timing information
  ws.on('pong', () => {
    handlePong(ws);
  });

  // Enhanced message handler with better application-level ping/pong support
  ws.on('message', (message) => {
    let data;
    
    // Parse message with error handling
    try {
      data = JSON.parse(message);
    } catch (error) {
      console.error('Invalid message format:', error.message);
      sendError(ws, 'invalid-message-format', 'Message must be valid JSON');
      return;
    }
    
    // Log received message types for debugging (except frequent messages)
    if (data.type !== 'ping' && data.type !== 'pong') {
      console.log(`Received message type: ${data.type} from user ${ws.userId || 'unknown'}`);
    }
    
    // Handle application-level ping
    if (data.type === 'ping') {
      // Respond with a pong containing the original timestamp and current timestamp
      try {
        const pongData = JSON.stringify({
          type: 'pong',
          clientTimestamp: data.timestamp,
          serverTimestamp: Date.now()
        });
        ws.send(pongData);
        return;
      } catch (error) {
        console.error('Error sending pong response:', error.message);
      }
    }
    
    // Handle application-level pong (client response to our ping)
    if (data.type === 'pong') {
      const receiveTime = Date.now();
      const roundTripTime = receiveTime - data.clientTimestamp;
      
      // Update connection quality metrics
      if (!ws.connectionQuality.appLatencies) {
        ws.connectionQuality.appLatencies = [];
      }
      
      ws.connectionQuality.appLatencies.push(roundTripTime);
      
      // Keep only last 10 measurements
      if (ws.connectionQuality.appLatencies.length > 10) {
        ws.connectionQuality.appLatencies.shift();
      }
      
      // Update history
      ws.pingHistory.push({
        status: 'app-pong-received',
        timestamp: receiveTime,
        latency: roundTripTime
      });
      
      return;
    }
    
    // Record message type in metrics
    serverMetrics.recordMessage(data.type || 'unknown');
    
    // Use a message handler map for faster processing instead of switch statement
    const handlers = {
      'join': handleJoin,
      'reconnect': handleReconnect,
      'offer': handleSignaling,
      'answer': handleSignaling,
      'ice-candidate': handleSignaling,
      'leave': handleLeave,
      'close-room': handleCloseRoom,
      'message': handleChatMessage,
      'host-command': handleHostCommand,
      // Make sure confirmation responses are handled
      'confirm-command': (ws, data) => {
        // Simply pass the confirmation to the host command handler
        console.log(`Received command confirmation for: ${data.command}`);
        handleHostCommand(ws, data);
      }
    };
    
    const handler = handlers[data.type];
    if (handler) {
      handler(ws, data);
    } else {
      console.warn(`Unknown message type: ${data.type}`);
      sendError(ws, 'unknown-message-type', `Unknown message type: ${data.type}`);
    }
  });

  // Handle connection close
  ws.on('close', () => {
    console.log(`WebSocket closed for ${ws.userId || 'unknown user'} in room ${ws.roomId || 'unknown'}`);
    handleDisconnection(ws);
    serverMetrics.recordDisconnection();
  });
  
  // Handle errors
  ws.on('error', (error) => {
    console.error(`WebSocket error for ${ws.userId || 'unknown user'}:`, error.message);
    // Don't terminate here, let the client try to reconnect
  });
});

// Handler for join room requests
function handleJoin(ws, data) {
  const { userId, roomId, userName, isHost } = data;
  
  if (!userId || !roomId || !userName) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters');
  }
  
  // Check room limits
  if (rooms.size >= LIMITS.MAX_ROOMS && !rooms.has(roomId)) {
    return sendError(ws, 'server-limit', 'Maximum number of rooms reached');
  }
  
  // Store connection info
  ws.userId = userId;
  ws.roomId = roomId;
  
  // Create room if it doesn't exist
  if (!rooms.has(roomId)) {
    console.log(`Creating new room: ${roomId}`);
    rooms.set(roomId, new Map());
  }
  
  const room = rooms.get(roomId);
  
  // Check users per room limit
  if (room.size >= LIMITS.MAX_USERS_PER_ROOM && !room.has(userId)) {
    return sendError(ws, 'room-full', 'Room has reached maximum capacity');
  }
  
  // Check if this is the first user (make them host if not specified)
  const shouldBeHost = isHost || room.size === 0;
  
  // Add user to room
  room.set(userId, {
    ws,
    userName,
    isHost: shouldBeHost,
    connected: true,
    joinedAt: Date.now(),
    audioEnabled: true,  // Add initial audio state
    videoEnabled: true   // Add initial video state
  });
  
  // Store session for reconnection
  userSessions.set(userId, {
    roomId,
    userName,
    isHost: shouldBeHost
  });
  
  // Update room activity
  updateRoomActivity(roomId);
  
  console.log(`User ${userName} (${userId}) joined room ${roomId} as ${shouldBeHost ? 'host' : 'participant'}`);
  
  // Notify others in room
  broadcastToRoom(
    roomId, 
    messageTemplates.userJoined(userId, userName, shouldBeHost),
    userId
  );
  
  // Send list of existing participants to new user
  const participants = Array.from(room.entries())
    .filter(([id]) => id !== userId)
    .map(([id, user]) => ({
      userId: id,
      userName: user.userName,
      isHost: user.isHost,
      audioEnabled: user.audioEnabled,
      videoEnabled: user.videoEnabled
    }));
  
  ws.send(messageTemplates.roomJoined(roomId, participants, shouldBeHost));
}

// Fix reconnection handling to properly restore host status
function handleReconnect(ws, data) {
  const { userId, roomId, userName } = data;
  
  if (!userId || !roomId) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for reconnection');
  }
  
  // Check if room exists
  if (!rooms.has(roomId)) {
    return sendError(ws, 'room-not-found', 'The room no longer exists');
  }
  
  // Check if user session exists
  const session = userSessions.get(userId);
  if (!session) {
    return sendError(ws, 'session-expired', 'Your session has expired, please rejoin');
  }
  
  // Update connection info
  ws.userId = userId;
  ws.roomId = roomId;
  
  const room = rooms.get(roomId);
  const existingUser = room.get(userId);
  
  if (existingUser) {
    const wasHost = existingUser.isHost;
    // Update user's connection while preserving media state
    const previousAudioState = existingUser.audioEnabled;
    const previousVideoState = existingUser.videoEnabled;
    
    // Clear any pending disconnect timeout
    if (existingUser.disconnectTimeout) {
      clearTimeout(existingUser.disconnectTimeout);
      existingUser.disconnectTimeout = null;
    }
    
    // IMPORTANT FIX: Ensure we attach the WebSocket object to the existing user record
    existingUser.ws = ws;
    existingUser.connected = true;
    existingUser.disconnectedAt = null;
    existingUser.audioEnabled = previousAudioState;
    existingUser.videoEnabled = previousVideoState;
    
    console.log(`User ${userName} (${userId}) reconnected to room ${roomId}, host status: ${existingUser.isHost}`);
    
    // FIX: Check for host status mismatch between session and room data
    if (session.isHost && !existingUser.isHost) {
      // Host role was transferred while disconnected - notify user
      ws.send(JSON.stringify({
        type: 'host-status-changed',
        isHost: false,
        message: 'Host role was transferred during your disconnection'
      }));
    } else if (!session.isHost && existingUser.isHost) {
      // FIX: Session might be out of sync with room data, update it
      session.isHost = true;
      console.log(`Updated session host status for ${userId} to match room data (isHost: true)`);
    }
    
    // Notify others about reconnection with media state
    broadcastToRoom(
      roomId, 
      messageTemplates.userReconnected(
        userId, userName, existingUser.isHost, previousAudioState, previousVideoState
      ), 
      userId
    );
    
    // Send current participants to reconnected user with their media states
    const participants = Array.from(room.entries())
      .filter(([id]) => id !== userId)
      .map(([id, user]) => ({
        userId: id,
        userName: user.userName,
        isHost: user.isHost,
        audioEnabled: user.audioEnabled,
        videoEnabled: user.videoEnabled
      }));
    
    ws.send(messageTemplates.roomRejoined(roomId, participants, existingUser.isHost));
  } else {
    // User not found in room but has valid session - handle as new join
    console.log(`User ${userName} (${userId}) not found in room ${roomId} but has valid session, rejoining as host: ${session.isHost}`);
    handleJoin(ws, {
      ...data,
      isHost: session.isHost
    });
  }
}

// Handler for signaling messages (offer, answer, ice-candidate)
function handleSignaling(ws, data) {
  const { type, targetUserId, roomId } = data;
  const userId = ws.userId;
  
  if (!userId || !targetUserId || !roomId) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for signaling');
  }
  
  // Check if room exists
  if (!rooms.has(roomId)) {
    return sendError(ws, 'room-not-found', 'The room does not exist');
  }
  
  // Get target user
  const room = rooms.get(roomId);
  const targetUser = room.get(targetUserId);
  
  if (!targetUser || !targetUser.connected) {
    return sendError(ws, 'user-not-found', 'The target user is not in the room or disconnected');
  }
  
  // Add sender information
  const senderUser = room.get(userId);
  data.senderId = userId;
  data.senderName = senderUser ? senderUser.userName : null;
  
  // Update room activity on any signaling
  updateRoomActivity(roomId);
  
  // Forward the message to the target user - pre-stringify for performance
  try {
    const jsonMessage = JSON.stringify(data);
    targetUser.ws.send(jsonMessage);
  } catch (error) {
    console.error(`Error forwarding ${type} to user ${targetUserId}:`, error.message);
    return sendError(ws, 'signaling-error', 'Failed to forward signaling message');
  }
}

// Improve host transfer when original host leaves
function handleLeave(ws, data) {
  const { userId, roomId, userName } = data;
  
  if (!userId || !roomId) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for leaving');
  }
  
  console.log(`User ${userName || userId} is leaving room ${roomId}`);
  
  // Check if user is host before removing
  const room = rooms.get(roomId);
  if (room) {
    const user = room.get(userId);
    const isHost = user && user.isHost;
    
    if (isHost) {
      console.log(`Host ${userId} is leaving room ${roomId}, selecting new host`);
      // Find a new host before removing current one
      const newHostId = findNewHostCandidate(room, userId);
      if (newHostId) {
        const newHost = room.get(newHostId);
        if (transferHostRole(roomId, userId, newHostId)) {
          // Notify all clients about host change
          broadcastToRoom(roomId, JSON.parse(messageTemplates.hostChanged(
            userId, newHostId, newHost.userName, true // permanent transfer
          )));
          
          console.log(`Host role successfully transferred from ${userId} to ${newHostId}`);
        }
      } else {
        console.log(`No suitable host found, room ${roomId} will be closed when empty`);
      }
    }
  }
  
  // Now remove the user
  removeUserFromRoom(userId, roomId);
  
  // Clear connection info
  ws.userId = null;
  ws.roomId = null;
  
  // Confirm leave to the user
  ws.send(messageTemplates.leaveConfirmed());
}

// Fix for "remove all participants" command in handleHostCommand function
function handleHostCommand(ws, data) {
  const { command, roomId, targetUserId, confirmationToken } = data;
  const userId = ws.userId;
  
  if (!userId || !roomId || !command) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for host command');
  }
  
  console.log(`Received host command '${command}' from ${userId} in room ${roomId}`);
  
  // Check if room exists
  if (!rooms.has(roomId)) {
    return sendError(ws, 'room-not-found', 'The room does not exist');
  }
  
  const room = rooms.get(roomId);
  const user = room.get(userId);
  
  // Verify user is in the room
  if (!user) {
    console.error(`User ${userId} not found in room ${roomId} when attempting host command`);
    return sendError(ws, 'not-in-room', 'You are not in this room');
  }
  
  // Verify host status with detailed error reporting
  if (!user.isHost) {
    console.error(`User ${userId} attempted host command '${command}' but is not a host`);
    return sendError(ws, 'not-authorized', 'Only the host can issue commands');
  }
  
  // Check rate limits
  if (!checkCommandRateLimit(userId, command)) {
    serverMetrics.recordError('host-command-rate-limit');
    return sendError(ws, 'rate-limited', `You've used this command too many times. Please wait before trying again.`);
  }
  
  // Check if confirmation is required for destructive commands
  if (HOST_COMMAND_LIMITS.CONFIRMATION_REQUIRED.includes(command)) {
    const expectedToken = `${userId}-${roomId}-${command}`;
    
    // Require confirmation token for destructive commands
    if (!confirmationToken || confirmationToken !== expectedToken) {
      console.log(`Requesting confirmation for command '${command}' from user ${userId}`);
      
      // First request: send confirmation required response
      ws.send(JSON.stringify({
        type: 'confirmation-required',
        command: command,
        confirmationToken: expectedToken,
        message: `Please confirm this potentially destructive action: ${command}`
      }));
      return;
    }
    
    // Log the destructive command execution
    console.log(`[SECURITY] Host ${userId} executed destructive command: ${command} in room ${roomId} with valid confirmation`);
  }
  
  // Process commands with additional validation
  switch (command) {
    case 'mute-all':
      // Check if there are actually other users to mute
      let muteCount = 0;
      
      // Update all participants' audio state
      room.forEach((participant, participantId) => {
        if (participantId !== userId && participant.audioEnabled) { // Don't mute the host and only count those who are unmuted
          participant.audioEnabled = false;
          muteCount++;
        }
      });
      
      if (muteCount === 0) {
        return sendError(ws, 'command-redundant', 'All participants are already muted');
      }
      
      // Broadcast command to all users
      broadcastToRoom(roomId, {
        type: 'host-command',
        command: 'mute-all',
        hostId: userId,
        hostName: user.userName
      });
      
      // Log the action
      console.log(`Host ${userId} muted all ${muteCount} participants in room ${roomId}`);
      break;
      
    case 'remove-all':
      let removalCount = 0;
      let participantsToRemove = [];
      
      // First collect all participants to remove (except host)
      for (const [participantId, participant] of room.entries()) {
        if (participantId !== userId) {
          participantsToRemove.push(participantId);
        }
      }
      
      // No participants to remove
      if (participantsToRemove.length === 0) {
        return sendError(ws, 'command-redundant', 'No other participants to remove');
      }
      
      console.log(`Host ${userId} removing all ${participantsToRemove.length} participants from room ${roomId}`);
      
      // Then remove each participant (separated to avoid modifying map during iteration)
      for (const participantId of participantsToRemove) {
        const participant = room.get(participantId);
        
        if (participant) {
          // Notify the participant before removing
          if (participant.ws && participant.ws.readyState === WebSocket.OPEN) {
            try {
              participant.ws.send(JSON.stringify({
                type: 'host-command',
                command: 'remove-user',
                hostId: userId,
                hostName: user.userName,
                reason: data.reason || 'Removed by host'
              }));
            } catch (error) {
              console.error(`Error notifying user ${participantId} about removal:`, error.message);
            }
          }
          
          // Remove from room and user sessions
          room.delete(participantId);
          userSessions.delete(participantId);
          removalCount++;
        }
      }
      
      // Log the action
      console.log(`[SECURITY] Host ${userId} successfully removed all ${removalCount} participants from room ${roomId}`);
      
      // Confirm to host
      ws.send(JSON.stringify({
        type: 'command-executed',
        command: 'remove-all',
        affectedCount: removalCount
      }));
      break;
      
    case 'remove-user':
      if (!targetUserId) {
        return sendError(ws, 'missing-parameters', 'Target user ID is required');
      }
      
      // Cannot remove self through this command
      if (targetUserId === userId) {
        return sendError(ws, 'invalid-operation', 'You cannot remove yourself with this command');
      }
      
      const targetUser = room.get(targetUserId);
      if (!targetUser) {
        return sendError(ws, 'user-not-found', 'The specified user is not in the room');
      }
      
      if (targetUser.ws && targetUser.ws.readyState === WebSocket.OPEN) {
        targetUser.ws.send(JSON.stringify({
          type: 'host-command',
          command: 'remove-user',
          hostId: userId,
          hostName: user.userName,
          reason: data.reason || 'Removed by host'
        }));
        targetUser.ws.terminate();
      }
      room.delete(targetUserId);
      userSessions.delete(targetUserId);
      
      // Notify others
      broadcastToRoom(roomId, {
        type: 'user-removed',
        userId: targetUserId,
        removedBy: userId,
        removedByName: user.userName,
        reason: data.reason || 'Removed by host'
      });
      
      // Log the action
      console.log(`Host ${userId} removed user ${targetUserId} from room ${roomId}`);
      break;
      
    default:
      return sendError(ws, 'invalid-command', `Unknown host command: ${command}`);
  }
}

// Apply the same fixes to handleCloseRoom
function handleCloseRoom(ws, data) {
  const { userId, roomId, userName } = data;
  
  if (!userId || !roomId) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for closing room');
  }
  
  // Check if room exists
  if (!rooms.has(roomId)) {
    return sendError(ws, 'room-not-found', 'The room does not exist');
  }
  
  const room = rooms.get(roomId);
  const user = room.get(userId);
  
  // Enhanced host status verification with detailed logging
  if (!user) {
    console.error(`User ${userId} not found in room ${roomId} when attempting to close room`);
    return sendError(ws, 'not-in-room', 'You are not in this room');
  }
  
  // Verify this is the active connection for this user
  const isActiveConnection = (user.ws === ws);
  if (!isActiveConnection) {
    console.error(`WebSocket mismatch for user ${userId} when closing room - updating reference`);
    user.ws = ws; // Update the reference to this current WebSocket
  }
  
  // Verify host status with fallback to session data
  if (!user.isHost) {
    const session = userSessions.get(userId);
    
    console.error(`User ${userId} attempted to close room but isHost=${user.isHost}, sessionHost=${session?.isHost || false}`);
    
    if (session && session.isHost) {
      // Fix inconsistency
      console.log(`Fixing host status inconsistency for ${userId} before closing room`);
      user.isHost = true;
    } else {
      return sendError(ws, 'not-authorized', 'Only the host can close the room');
    }
  }
  
  console.log(`Host ${userName || userId} is closing room ${roomId}`);
  
  // Notify all participants that the room is closed
  broadcastToRoom(roomId, messageTemplates.roomClosed(roomId, userId, userName));
  
  // Terminate all WebSocket connections in the room
  room.forEach((participant, participantId) => {
    if (participant.ws && participant.ws.readyState === WebSocket.OPEN) {
      participant.ws.terminate();
    }
    userSessions.delete(participantId);
  });
  
  // Delete the room
  rooms.delete(roomId);
  roomQueue.remove(roomId);
  
  // Confirm to the host
  ws.send(JSON.stringify({
    type: 'room-close-confirmed'
  }));
  
  // Finally terminate the host's connection
  ws.terminate();
}

// Handler for chat messages and media updates
function handleChatMessage(ws, data) {
  const { roomId, message, isPrivate, targetUserId } = data;
  const userId = ws.userId;
  
  if (!userId || !roomId) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for chat message');
  }
  
  // Check if room exists
  if (!rooms.has(roomId)) {
    return sendError(ws, 'room-not-found', 'The room does not exist');
  }
  
  const room = rooms.get(roomId);
  const sender = room.get(userId);
  
  if (!sender) {
    return sendError(ws, 'not-in-room', 'You are not in this room');
  }

  // Handle media state updates
  if (message && message.type === 'media-update') {
    // Update the user's media state in the room
    if (message.kind === 'audio') {
      sender.audioEnabled = message.enabled;
    } else if (message.kind === 'video') {
      sender.videoEnabled = message.enabled;
    }
    
    // Create media update message
    const mediaUpdateMsg = {
      type: 'message',
      senderId: userId,
      userName: sender.userName,
      message: message
    };
    
    // Broadcast to all users in the room
    broadcastToRoom(roomId, mediaUpdateMsg, userId);
    return;
  }
  
  // Handle regular chat messages
  if (!message) {
    return sendError(ws, 'missing-parameters', 'Message content is required');
  }
  
  // Create message object
  const messageObj = {
    type: 'chat-message',
    senderId: userId,
    senderName: sender.userName,
    message: message,
    timestamp: Date.now(),
    isPrivate: !!isPrivate
  };
  
  if (isPrivate && targetUserId) {
    // Send private message to specific user
    const targetUser = room.get(targetUserId);
    if (!targetUser || !targetUser.connected) {
      return sendError(ws, 'user-not-found', 'The target user is not in the room');
    }
    
    // Send to target user
    targetUser.ws.send(JSON.stringify(messageObj));
    
    // Also send back to sender for confirmation
    ws.send(JSON.stringify(messageObj));
  } else {
    // Broadcast to all users in the room
    broadcastToRoom(roomId, messageObj);
  }
}

// Start the server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`FastMeet server running on port ${PORT}`);
  console.log(`WebSocket server is ready for connections`);
});

// Handle server shutdown gracefully
process.on('SIGINT', () => {
  console.log('Shutting down server...');
  
  // Clear all intervals
  clearInterval(pingIntervalId);
  clearInterval(roomCleanupInterval);
  
  // Notify all clients about server shutdown
  const shutdownMessage = JSON.stringify({
    type: 'server-shutdown',
    message: 'Server is shutting down'
  });
  
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(shutdownMessage);
    }
  });
  
  // Close the WebSocket server
  wss.close(() => {
    console.log('WebSocket server closed');
    // Close the HTTP server
    server.close(() => {
      console.log('HTTP server closed');
      process.exit(0);
    });
  });
});

module.exports = { app, server };