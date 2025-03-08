const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const dotenv = require('dotenv');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

// Load environment variables
dotenv.config();

// Initialize Express app and HTTP server
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Enable CORS and serve static files
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

// Add a route for the main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Add a route for joining a specific room
app.get('/join/:roomId', (req, res) => {
  res.redirect(`/?room=${req.params.roomId}`);
});

// Data structures to store active rooms and their participants
const rooms = new Map();
const userSessions = new Map(); // Track user sessions for reconnection

// Set ping interval to keep connections alive
const PING_INTERVAL = 30000; // 30 seconds
const CONNECTION_TIMEOUT = 10000; // 10 seconds for connection timeout

// Ping all clients periodically to keep connections alive
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      // Connection is dead, handle disconnection
      handleDisconnection(ws);
      return ws.terminate();
    }

    ws.isAlive = false;
    ws.ping();
  });
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

// Helper function to broadcast message to all users in a room
function broadcastToRoom(roomId, message, excludeUserId = null) {
  const room = rooms.get(roomId);
  if (room) {
    let sentCount = 0;
    room.forEach((user, id) => {
      if (id !== excludeUserId && user.connected && user.ws.readyState === WebSocket.OPEN) {
        try {
          user.ws.send(JSON.stringify(message));
          sentCount++;
        } catch (error) {
          console.error(`Error sending message to user ${id}:`, error.message);
        }
      }
    });
    return sentCount;
  }
  return 0;
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

  // Handle incoming messages
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
    
    // Log message type for debugging
    console.log(`Received message type: ${data.type}`);
    
    // Handle different message types
    switch (data.type) {
      case 'join':
        handleJoin(ws, data);
        break;
        
      case 'reconnect':
        handleReconnect(ws, data);
        break;
        
      case 'offer':
      case 'answer':
      case 'ice-candidate':
        handleSignaling(ws, data);
        break;
        
      case 'leave':
        handleLeave(ws, data);
        break;
        
      case 'close-room':
        handleCloseRoom(ws, data);
        break;
        
      case 'message':
        handleChatMessage(ws, data);
        break;
        
      default:
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
  
  // Forward the message to the target user
  try {
    targetUser.ws.send(JSON.stringify(data));
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

// Helper function to send error messages
function sendError(ws, code, message) {
  try {
    ws.send(JSON.stringify({
      type: 'error',
      error: code,
      message: message
    }));
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
  
  // Notify all clients about server shutdown
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({
        type: 'server-shutdown',
        message: 'Server is shutting down'
      }));
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