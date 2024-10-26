import express from 'express';
import { WebSocketServer } from 'ws';
import { createClient } from 'redis';
import { PrismaClient } from '@prisma/client';

const app = express();
const httpserver = app.listen(8080, () => {
  console.log('Server is running on port 8080');
});

// Initialize WebSocket server
const socket = new WebSocketServer({ server: httpserver });

// Create Redis client and handle connection errors
const redis = createClient({ url: 'redis://127.0.0.1:6379' });
redis.on('error', (err) => console.error('Redis Client Error', err));

// Create Prisma client
const prisma = new PrismaClient();

// IoTData type definition
interface IoTData {
  timestamp: string;
  value: number;
}

// Simple data validation function
function isValidData(data: IoTData): boolean {
  return !!data.timestamp && typeof data.value === 'number'; 
}

(async () => {
  try {
    await redis.connect();
    console.log('Connected to Redis');

    // Start ingesting Redis stream data into PostgreSQL
    await ingestDataFromRedis();
  } catch (error) {
    console.error('Redis connection error:', error);
  }
})();

// Function to ingest data from Redis stream into PostgreSQL
async function ingestDataFromRedis() {
  const streamName = 'iot-data';
  let lastId = '0'; // Start reading from the beginning

  while (true) {
    try {
      // Read messages from Redis stream
      const messages = await redis.xRead(
        { key: streamName, id: lastId },
        { BLOCK: 0, COUNT: 1 } // Block until new messages arrive
      );

      // Check if messages are returned and process them
      if (messages && messages.length > 0) {
        for (const message of messages) {
          const streamName = message.name; // Extract stream name if needed
          
          // Iterate over the inner messages
          for (const entry of message.messages) {
            const id = entry.id; // Message ID
            const data = entry.message; // The actual message data

            // Extracting timestamp and value from the message data
            const timestamp = data.timestamp; // Adjust this according to your keys
            const value = Number(data.value); // Adjust this according to your keys

            const ioTData: IoTData = {
              timestamp: timestamp,
              value: value,
            };

            // Log the entire flow
            console.log(`Data coming from Redis: ${JSON.stringify(ioTData)}`);

            // Validate and store in PostgreSQL
            if (isValidData(ioTData)) {
              await prisma.iotData.create({
                data: {
                  timestamp: new Date(ioTData.timestamp), // Ensure the timestamp is in a Date format
                  value: ioTData.value,
                },
              });
              console.log('Data saved to PostgreSQL:', ioTData);

              // Update lastId to mark the last processed message
              lastId = id;

              // Delete the message from the Redis stream after processing
              await redis.xDel(streamName, id); // Delete the processed message
              console.log(`Deleted message with ID ${id} from stream ${streamName}`);
            } else {
              console.warn('Invalid data received:', ioTData);
            }
          }
        }
      }
    } catch (err) {
      console.error('Error reading from Redis stream:', err);
    }
  }
}

// WebSocket connection handler
socket.on('connection', (client) => {
  console.log('WebSocket client connected');

  client.on('error', console.error);

  // Handle incoming messages from clients
  client.on('message', async (message: Buffer | string) => {
    try {
      const parsedMessage = typeof message === 'string' ? message : message.toString(); // Convert to string if necessary
      const data: IoTData = JSON.parse(parsedMessage); // Parse JSON

      // Log the data received from the WebSocket client
      console.log(`Data coming from WebSocket: ${parsedMessage}`);

      if (isValidData(data)) {
        // 1. Store data in Redis Stream
        console.log(data);

        await redis.xAdd('iot-data', '*', {
          timestamp: data.timestamp,
          value: data.value.toString(),
        });
        
        console.log('Data added to Redis Stream:', data);
      } else {
        console.warn('Invalid data received from WebSocket:', data);
      }

      // Send a message to the client
      client.send('Message from server');
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });
});
