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

// Initialize Express app and HTTP server
const app = express();
const server = http.createServer(app);

// LRU Cache implementation for room data
class LRUCache {
  constructor(maxSize = 1000) {
    this.maxSize = maxSize;
    this.cache = new Map();
    this.keys = [];
  }
  
  get(key) {
    if (!this.cache.has(key)) return null;
    
    // Move to most recently used
    this.keys = this.keys.filter(k => k !== key);
    this.keys.push(key);
    
    return this.cache.get(key);
  }
  
  set(key, value, ttl = 0) {
    // Update existing or add new
    if (this.cache.has(key)) {
      this.keys = this.keys.filter(k => k !== key);
    } else if (this.keys.length >= this.maxSize) {
      // Evict least recently used
      const lru = this.keys.shift();
      this.cache.delete(lru);
    }
    
    this.keys.push(key);
    this.cache.set(key, value);
    
    // Optional TTL
    if (ttl > 0) {
      setTimeout(() => {
        if (this.cache.has(key)) {
          this.delete(key);
        }
      }, ttl);
    }
  }
  
  delete(key) {
    this.cache.delete(key);
    this.keys = this.keys.filter(k => k !== key);
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
  
  getStats() {
    return {
      uptime: Math.floor((Date.now() - this.startTime) / 1000),
      connections: this.connections,
      messages: this.messages,
      rooms: {
        total: rooms.size,
        active: Array.from(rooms.values()).filter(room => room.size > 0).length
      },
      errors: this.errors,
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
    rooms: stats.rooms.total
  });
});

// Add metrics endpoint
app.get('/metrics', (req, res) => {
  res.json(serverMetrics.getStats());
});

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

// Improved broadcast function with Set-based lookups
function broadcastToRoom(roomId, message, excludeUserId = null) {
  const room = rooms.get(roomId);
  if (!room) return 0;
  
  // Update room activity
  updateRoomActivity(roomId);
  
  // Create excluded set for faster lookups
  const excluded = new Set(excludeUserId ? [excludeUserId] : []);
  
  // Convert message to JSON once if not already a string
  const jsonMessage = typeof message === 'string' ? message : JSON.stringify(message);
  let sentCount = 0;
  
  // Send to all connected users not in excluded set
  for (const [userId, user] of room.entries()) {
    if (!excluded.has(userId) && user.connected && user.ws.readyState === WebSocket.OPEN) {
      try {
        user.ws.send(jsonMessage);
        sentCount++;
      } catch (error) {
        console.error(`Error sending to ${userId}:`, error.message);
        user.connected = false; // Mark as disconnected on error
      }
    }
  }
  
  return sentCount;
}

// Add host transfer mechanism
function transferHostRole(roomId, oldHostId, newHostId) {
  const room = rooms.get(roomId);
  if (!room) return false;

  const oldHost = room.get(oldHostId);
  const newHost = room.get(newHostId);
  
  if (!oldHost || !newHost) return false;

  // Update host status
  oldHost.isHost = false;
  newHost.isHost = true;

  // Update session data
  const oldHostSession = userSessions.get(oldHostId);
  const newHostSession = userSessions.get(newHostId);
  
  if (oldHostSession) oldHostSession.isHost = false;
  if (newHostSession) newHostSession.isHost = true;

  return true;
}

// Add function to find the best candidate for new host
function findNewHostCandidate(room, excludeUserId) {
  // Sort users by connection time (oldest first) and connection status
  const candidates = Array.from(room.entries())
    .filter(([userId, user]) => userId !== excludeUserId)
    .sort((a, b) => {
      // Prioritize connected users
      if (a[1].connected && !b[1].connected) return -1;
      if (!a[1].connected && b[1].connected) return 1;
      // Then sort by join time
      return a[1].joinedAt - b[1].joinedAt;
    });

  return candidates.length > 0 ? candidates[0][0] : null;
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

// Enhanced ping system
const pingAllClients = () => {
  let activePings = 0;
  let closedConnections = 0;
  
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      closedConnections++;
      handleDisconnection(ws);
      ws.terminate();
      return;
    }
    
    ws.isAlive = false;
    ws.ping();
    activePings++;
  });
  
  // Only log if there's activity to reduce noise
  if (closedConnections > 0) {
    console.log(`Ping cycle: ${activePings} active, ${closedConnections} terminated`);
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
          messageTemplates.hostChanged(userId, newHostId, newHost.userName)
        );
        console.log(`Host role transferred from ${userId} to ${newHostId} in room ${roomId}`);
      }
    }
  }

  // Set a shorter timeout for host reconnection
  const timeoutDuration = wasHost ? 15000 : 30000; // 15 seconds for host, 30 for others

  // Create a unique timeout key for this disconnection
  const timeoutKey = `${userId}-${Date.now()}`;
  
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
  ws.isAlive = true;
  ws.userId = null;
  ws.roomId = null;

  // Update metrics
  serverMetrics.recordConnection();
  
  // Handle pong messages to keep connection alive
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  // Handle incoming messages with optimized message handler
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
      'host-command': handleHostCommand
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

// Handler for reconnect requests
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
    
    existingUser.ws = ws;
    existingUser.connected = true;
    existingUser.disconnectedAt = null;
    existingUser.audioEnabled = previousAudioState;
    existingUser.videoEnabled = previousVideoState;
    
    console.log(`User ${userName} (${userId}) reconnected to room ${roomId}`);
    
    // If this user was previously the host, check if the role was transferred
    if (session.isHost && !existingUser.isHost) {
      // Notify the reconnected user that they are no longer the host
      ws.send(JSON.stringify({
        type: 'host-status-changed',
        isHost: false,
        message: 'Host role was transferred during your disconnection'
      }));
    }
    
    // Notify others about reconnection with media state
    broadcastToRoom(
      roomId, 
      messageTemplates.userReconnected(
        userId, userName, wasHost, previousAudioState, previousVideoState
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

// Handler for leave room requests
function handleLeave(ws, data) {
  const { userId, roomId, userName } = data;
  
  if (!userId || !roomId) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for leaving');
  }
  
  console.log(`User ${userName || userId} is leaving room ${roomId}`);
  
  // Remove user from room
  removeUserFromRoom(userId, roomId);
  
  // Clear connection info
  ws.userId = null;
  ws.roomId = null;
  
  // Confirm leave to the user
  ws.send(messageTemplates.leaveConfirmed());
}

// Handler for close room requests (host only)
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
  
  // Verify user is the host
  if (!user || !user.isHost) {
    return sendError(ws, 'not-authorized', 'Only the host can close the room');
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

// Handler for host commands
function handleHostCommand(ws, data) {
  const { command, roomId, targetUserId } = data;
  const userId = ws.userId;
  
  if (!userId || !roomId || !command) {
    return sendError(ws, 'missing-parameters', 'Missing required parameters for host command');
  }
  
  // Check if room exists
  if (!rooms.has(roomId)) {
    return sendError(ws, 'room-not-found', 'The room does not exist');
  }
  
  const room = rooms.get(roomId);
  const user = room.get(userId);
  
  // Verify user is the host
  if (!user || !user.isHost) {
    return sendError(ws, 'not-authorized', 'Only the host can issue commands');
  }
  
  // Process command
  switch (command) {
    case 'mute-all':
      // Update all participants' audio state
      room.forEach((participant, participantId) => {
        if (participantId !== userId) { // Don't mute the host
          participant.audioEnabled = false;
        }
      });
      
      // Broadcast command to all users
      broadcastToRoom(roomId, {
        type: 'host-command',
        command: 'mute-all',
        hostId: userId,
        hostName: user.userName
      });
      break;
      
    case 'remove-all':
      // Terminate all connections except host
      room.forEach((participant, participantId) => {
        if (participantId !== userId) {
          if (participant.ws && participant.ws.readyState === WebSocket.OPEN) {
            participant.ws.send(JSON.stringify({
              type: 'host-command',
              command: 'remove-all',
              hostId: userId,
              hostName: user.userName
            }));
            participant.ws.terminate();
          }
          room.delete(participantId);
          userSessions.delete(participantId);
        }
      });
      break;
      
    case 'remove-user':
      if (!targetUserId) {
        return sendError(ws, 'missing-parameters', 'Target user ID is required');
      }
      
      const targetUser = room.get(targetUserId);
      if (targetUser) {
        if (targetUser.ws && targetUser.ws.readyState === WebSocket.OPEN) {
          targetUser.ws.send(JSON.stringify({
            type: 'host-command',
            command: 'remove-user',
            hostId: userId,
            hostName: user.userName
          }));
          targetUser.ws.terminate();
        }
        room.delete(targetUserId);
        userSessions.delete(targetUserId);
        
        // Notify others
        broadcastToRoom(roomId, {
          type: 'user-removed',
          userId: targetUserId,
          removedBy: userId
        });
      }
      break;
      
    default:
      return sendError(ws, 'invalid-command', `Unknown host command: ${command}`);
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