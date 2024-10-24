import express from 'express';
import { WebSocketServer } from 'ws';
import { createClient } from 'redis'; // ESM import for Redis

const app = express();
const httpserver = app.listen(8080, () => {
  console.log('Server is running on port 8080');
});

// Initialize WebSocket server
const socket = new WebSocketServer({ server: httpserver });

// Create Redis client and handle connection errors
const redis = createClient({ url: 'redis://127.0.0.1:6379' });
redis.on('error', (err) => console.error('Redis Client Error', err));

(async () => {
  try {
    await redis.connect();
    console.log('Connected to Redis');
  } catch (error) {
    console.error('Redis connection error:', error);
  }
})();

// Express route handler
app.get('/', (req, res) => {
  res.send('Hi there! This is the Express server.');
});

// WebSocket connection handler
socket.on('connection', (client) => {
  client.on('error', console.error);

  // Handle incoming messages from clients
  client.on('message', async (message: Buffer | string) => {
    try {
      const parsedMessage = typeof message === 'string' ? message : message.toString(); // Convert to string if necessary
      const data: IoTData = JSON.parse(parsedMessage); // Parse JSON

      if (isValidData(data)) {
        await redis.xAdd('iot-data', '*', {
          timestamp: data.timestamp,
          value: data.value.toString(), // Convert value to string
        });
        console.log('Data added to Redis Stream:', data);
      } else {
        console.warn('Invalid data received:', data);
      }

      console.log(`Received data from client: ${parsedMessage}`);
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });

  // Send message to client
  client.send('Message from server');
});

// IoTData type definition
interface IoTData {
  timestamp: string;
  value: number;
}

// Simple data validation function
function isValidData(data: IoTData): boolean {
  return !!data.timestamp && !!data.value; // Ensure both fields are present
}
