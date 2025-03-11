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

// Initialize Express app and HTTP server
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ 
  server,
  perMessageDeflate: {
    zlibDeflateOptions: {
      chunkSize: 1024,
      memLevel: 7,
      level: 3
    },
    zlibInflateOptions: {
      chunkSize: 10 * 1024
    },
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    concurrencyLimit: 10,
    threshold: 1024
  }
});

// Enable CORS and use compression for all responses
app.use(cors());
app.use(compression());

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

// Cache for frequently accessed data
const roomCache = new Map();
const clearRoomCache = (roomId) => roomCache.delete(roomId);

// Data structures to store active rooms and their participants
const rooms = new Map();
const userSessions = new Map(); // Track user sessions for reconnection

// Set ping interval to keep connections alive
const PING_INTERVAL = 30000; // 30 seconds
const CONNECTION_TIMEOUT = 10000; // 10 seconds for connection timeout

// Add room activity tracking
const ROOM_CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutes
const ROOM_INACTIVE_THRESHOLD = 2 * 60 * 1000; // 2 minutes
const roomActivity = new Map();

// Add room activity monitoring system
function updateRoomActivity(roomId) {
    roomActivity.set(roomId, Date.now());
}

// Add room cleanup interval
const roomCleanupInterval = setInterval(() => {
    const now = Date.now();
    let cleanedRooms = 0;
    
    rooms.forEach((room, roomId) => {
        const lastActivity = roomActivity.get(roomId) || 0;
        const isInactive = now - lastActivity > ROOM_INACTIVE_THRESHOLD;
        
        if (isInactive) {
            // Check if room is actually empty or has only disconnected users
            const hasActiveUsers = Array.from(room.values()).some(user => 
                user.connected && user.ws.readyState === WebSocket.OPEN
            );
            
            if (!hasActiveUsers) {
                console.log(`Cleaning up inactive room: ${roomId}, last activity: ${new Date(lastActivity).toISOString()}`);
                
                // Notify any remaining users about room closure
                broadcastToRoom(roomId, {
                    type: 'room-closed',
                    roomId: roomId,
                    reason: 'Room closed due to inactivity'
                });
                
                // Clean up all user sessions for this room
                room.forEach((user, userId) => {
                    userSessions.delete(userId);
                });
                
                // Remove room and its activity record
                rooms.delete(roomId);
                roomActivity.delete(roomId);
                cleanedRooms++;
            }
        }
    });
    
    if (cleanedRooms > 0) {
        console.log(`Cleaned up ${cleanedRooms} inactive rooms`);
    }
}, ROOM_CLEANUP_INTERVAL);

// Create a more efficient ping system with separate ping handling logic
const pingIntervalId = setInterval(() => {
  let totalPings = 0;
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      // Connection is dead, handle disconnection
      handleDisconnection(ws);
      return ws.terminate();
    }

    ws.isAlive = false;
    ws.ping();
    totalPings++;
  });
  
  if (totalPings > 0) {
    console.log(`Pinged ${totalPings} clients`);
  }
}, PING_INTERVAL);

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

// Update handleDisconnection function to handle host disconnection better
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
    broadcastToRoom(roomId, {
        type: 'user-disconnected',
        userId: userId,
        userName: user.userName,
        temporary: true,
        wasHost: wasHost
    }, userId);

    // If the disconnected user was the host, immediately try to find a new host
    if (wasHost) {
        const newHostId = findNewHostCandidate(room, userId);
        if (newHostId) {
            if (transferHostRole(roomId, userId, newHostId)) {
                const newHost = room.get(newHostId);
                broadcastToRoom(roomId, {
                    type: 'host-changed',
                    oldHostId: userId,
                    newHostId: newHostId,
                    newHostName: newHost.userName
                });
                console.log(`Host role transferred from ${userId} to ${newHostId} in room ${roomId}`);
            }
        }
    }

    // Set a shorter timeout for host reconnection
    const timeoutDuration = wasHost ? 15000 : 30000; // 15 seconds for host, 30 for others

    setTimeout(() => {
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
                        broadcastToRoom(roomId, {
                            type: 'host-changed',
                            oldHostId: userId,
                            newHostId: newHostId,
                            newHostName: newHost.userName,
                            permanent: true
                        });
                    }
                } else {
                    // No suitable host found, close the room
                    broadcastToRoom(roomId, {
                        type: 'room-closed',
                        roomId: roomId,
                        reason: 'No active participants to transfer host role'
                    });
                    rooms.delete(roomId);
                }
            }
        }
    }, timeoutDuration);
}

// Helper function to remove user from room
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
    clearRoomCache(roomId);
    
    // Validate room after user removal
    const isRoomValid = validateRoom(roomId);
    
    if (isRoomValid) {
        // Only broadcast if room still exists and has users
        broadcastToRoom(roomId, {
            type: 'user-left',
            userId: userId,
            userName: userName
        });
        
        // Handle host transfer if needed
        if (isHost && room.size > 0) {
            const [newHostId, newHost] = Array.from(room.entries())[0];
            newHost.isHost = true;
            
            broadcastToRoom(roomId, {
                type: 'new-host',
                userId: newHostId,
                userName: newHost.userName
            });
        }
    }
  }
}

// Optimized broadcast function with message batching capability
function broadcastToRoom(roomId, message, excludeUserId = null) {
  const room = rooms.get(roomId);
  if (!room) return 0;
  
  // Update room activity on broadcast
  updateRoomActivity(roomId);
  
  // Check if we can use a cached participant list
  let cachedReceivers = roomCache.get(`${roomId}-receivers-${excludeUserId || 'none'}`);
  let receivers;
  
  if (!cachedReceivers) {
    // Create a list of receivers (participants who should receive the message)
    receivers = Array.from(room.entries())
      .filter(([id, user]) => id !== excludeUserId && user.connected)
      .map(([id, user]) => user.ws);
      
    // Cache this list for future broadcasts (with a short TTL)
    roomCache.set(`${roomId}-receivers-${excludeUserId || 'none'}`, receivers);
    setTimeout(() => roomCache.delete(`${roomId}-receivers-${excludeUserId || 'none'}`), 1000);
  } else {
    receivers = cachedReceivers;
  }
  
  // Convert message to JSON once instead of for each client
  const jsonMessage = JSON.stringify(message);
  let sentCount = 0;
  
  // Send message to all receivers
  for (const ws of receivers) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(jsonMessage);
        sentCount++;
      } catch (error) {
        console.error('Error sending message:', error.message);
      }
    }
  }
  
  return sentCount;
}

// WebSocket connection handler
wss.on('connection', (ws) => {
  // Initialize connection state
  ws.isAlive = true;
  ws.userId = null;
  ws.roomId = null;
  
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
      ws.send(JSON.stringify({
        type: 'error',
        error: 'invalid-message-format',
        message: 'Message must be valid JSON'
      }));
      return;
    }
    
    // Use a message handler map for faster processing instead of switch statement
    const handlers = {
      'join': handleJoin,
      'reconnect': handleReconnect,
      'offer': handleSignaling,
      'answer': handleSignaling,
      'ice-candidate': handleSignaling,
      'leave': handleLeave,
      'close-room': handleCloseRoom,
      'message': handleChatMessage
    };
    
    const handler = handlers[data.type];
    if (handler) {
      handler(ws, data);
    } else {
      console.warn(`Unknown message type: ${data.type}`);
      ws.send(JSON.stringify({
        type: 'error',
        error: 'unknown-message-type',
        message: `Unknown message type: ${data.type}`
      }));
    }
  });

  // Handle connection close
  ws.on('close', () => {
    console.log(`WebSocket closed for ${ws.userId || 'unknown user'} in room ${ws.roomId || 'unknown'}`);
    handleDisconnection(ws);
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
  
  // Store connection info
  ws.userId = userId;
  ws.roomId = roomId;
  
  // Create room if it doesn't exist
  if (!rooms.has(roomId)) {
    console.log(`Creating new room: ${roomId}`);
    rooms.set(roomId, new Map());
  }
  
  const room = rooms.get(roomId);
  
  // Check if this is the first user (make them host if not specified)
  const shouldBeHost = isHost || room.size === 0;
  
  // Add user to room
  room.set(userId, {
    ws,
    userName,
    isHost: shouldBeHost,
    connected: true,
    joinedAt: Date.now(),
    audioEnabled: true  // Add initial audio state
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
  broadcastToRoom(roomId, {
    type: 'user-joined',
    userId: userId,
    userName: userName,
    isHost: shouldBeHost
  }, userId);
  
  // Send list of existing participants to new user
  const participants = Array.from(room.entries())
    .filter(([id]) => id !== userId)
    .map(([id, user]) => ({
      userId: id,
      userName: user.userName,
      isHost: user.isHost,
      audioEnabled: user.audioEnabled  // Include audio state
    }));
  
  ws.send(JSON.stringify({
    type: 'room-joined',
    roomId,
    participants,
    isHost: shouldBeHost
  }));
}

// Update handleReconnect to handle host reconnection
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
        // Update user's connection
        existingUser.ws = ws;
        existingUser.connected = true;
        existingUser.disconnectedAt = null;
        
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
        
        // Notify others about reconnection
        broadcastToRoom(roomId, {
            type: 'user-reconnected',
            userId: userId,
            userName: userName,
            wasHost: wasHost
        }, userId);
        
        // Send current participants to reconnected user
        const participants = Array.from(room.entries())
            .filter(([id]) => id !== userId)
            .map(([id, user]) => ({
                userId: id,
                userName: user.userName,
                isHost: user.isHost
            }));
        
        ws.send(JSON.stringify({
            type: 'room-rejoined',
            roomId,
            participants,
            isHost: existingUser.isHost
        }));
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
  ws.send(JSON.stringify({
    type: 'leave-confirmed'
  }));
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
  broadcastToRoom(roomId, {
    type: 'room-closed',
    roomId: roomId,
    hostId: userId,
    hostName: userName
  });
  
  // Terminate all WebSocket connections in the room
  room.forEach((participant, participantId) => {
    if (participant.ws && participant.ws.readyState === WebSocket.OPEN) {
      participant.ws.terminate();
    }
    userSessions.delete(participantId);
  });
  
  // Delete the room
  rooms.delete(roomId);
  
  // Confirm to the host
  ws.send(JSON.stringify({
    type: 'room-close-confirmed'
  }));
  
  // Finally terminate the host's connection
  ws.terminate();
}

// Handler for chat messages
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
    // Update the user's audio state in the room
    if (message.kind === 'audio') {
        sender.audioEnabled = message.enabled;
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

// Optimized error sending function
function sendError(ws, code, message) {
  // Cache commonly used error responses
  const errorKey = `${code}-${message}`;
  let errorJson = roomCache.get(errorKey);
  
  if (!errorJson) {
    errorJson = JSON.stringify({
      type: 'error',
      error: code,
      message: message
    });
    roomCache.set(errorKey, errorJson);
  }
  
  try {
    ws.send(errorJson);
  } catch (error) {
    console.error('Error sending error message:', error.message);
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

// Add validation to prevent zombie room creation
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
        return false;
    }
    
    return true;
} 