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

// Helper function to handle disconnection
function handleDisconnection(ws) {
  const { userId, roomId } = ws;
  if (roomId && userId) {
    // Mark user as temporarily disconnected but don't remove yet
    const room = rooms.get(roomId);
    if (room) {
      const user = room.get(userId);
      if (user) {
        user.connected = false;
        user.disconnectedAt = Date.now();
        
        // Notify others about temporary disconnection
        broadcastToRoom(roomId, {
          type: 'user-disconnected',
          userId: userId,
          userName: user.userName,
          temporary: true
        }, userId);
        
        // Schedule final removal if not reconnected within 30 seconds
        setTimeout(() => {
          const currentRoom = rooms.get(roomId);
          if (currentRoom) {
            const currentUser = currentRoom.get(userId);
            if (currentUser && !currentUser.connected) {
              // User didn't reconnect, remove permanently
              removeUserFromRoom(userId, roomId);
            }
          }
        }, 30000); // 30 seconds to reconnect
      }
    }
  }
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
    
    // Notify others in room
    broadcastToRoom(roomId, {
      type: 'user-left',
      userId: userId,
      userName: userName
    });
    
    // If host left, assign a new host or close the room
    if (isHost && room.size > 0) {
      // Assign the first remaining user as host
      const [newHostId, newHost] = Array.from(room.entries())[0];
      newHost.isHost = true;
      
      // Notify about new host
      broadcastToRoom(roomId, {
        type: 'new-host',
        userId: newHostId,
        userName: newHost.userName
      });
    }
    
    // Clean up empty rooms
    if (room.size === 0) {
      rooms.delete(roomId);
      console.log(`Room ${roomId} deleted (empty)`);
    }
  }
}

// Optimized broadcast function with message batching capability
function broadcastToRoom(roomId, message, excludeUserId = null) {
  const room = rooms.get(roomId);
  if (!room) return 0;
  
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
    joinedAt: Date.now()
  });
  
  // Store session for reconnection
  userSessions.set(userId, {
    roomId,
    userName,
    isHost: shouldBeHost
  });
  
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
      isHost: user.isHost
    }));
  
  ws.send(JSON.stringify({
    type: 'room-joined',
    roomId,
    participants,
    isHost: shouldBeHost
  }));
}

// Handler for reconnection attempts
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
    // Update user's connection
    existingUser.ws = ws;
    existingUser.connected = true;
    existingUser.disconnectedAt = null;
    
    console.log(`User ${userName} (${userId}) reconnected to room ${roomId}`);
    
    // Notify others about reconnection
    broadcastToRoom(roomId, {
      type: 'user-reconnected',
      userId: userId,
      userName: userName
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
  } else {
    // User exists in session but not in room (rare case)
    // Add them back to the room
    room.set(userId, {
      ws,
      userName: session.userName,
      isHost: session.isHost,
      connected: true,
      joinedAt: Date.now()
    });
    
    console.log(`User ${session.userName} (${userId}) re-added to room ${roomId}`);
    
    // Notify others in room
    broadcastToRoom(roomId, {
      type: 'user-joined',
      userId: userId,
      userName: session.userName,
      isHost: session.isHost
    }, userId);
    
    // Send list of existing participants
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
      isHost: session.isHost
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
  
  // Clear all user sessions for this room
  room.forEach((user, id) => {
    userSessions.delete(id);
  });
  
  // Delete the room
  rooms.delete(roomId);
  
  // Confirm to the host
  ws.send(JSON.stringify({
    type: 'room-close-confirmed'
  }));
}

// Handler for chat messages
function handleChatMessage(ws, data) {
  const { roomId, message, isPrivate, targetUserId } = data;
  const userId = ws.userId;
  
  if (!userId || !roomId || !message) {
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
  
  // Clear the ping interval
  clearInterval(pingIntervalId);
  
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