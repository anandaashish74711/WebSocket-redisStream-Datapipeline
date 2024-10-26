import express from 'express';
import { WebSocketServer } from 'ws';
import { createClient, RedisClientType } from 'redis';
import { PrismaClient } from '@prisma/client';

const app = express();
const httpserver = app.listen(8080, () => {
  console.log('Server is running on port 8080');
});

// Initialize WebSocket server
const socket = new WebSocketServer({ server: httpserver });

// Create Redis client
const redis: RedisClientType = createClient({ url: 'redis://127.0.0.1:6379' });

(async () => {
  try {
    await redis.connect();
    console.log('Redis client connected successfully');
    ingestDataFromRedis(); // Start background ingestion after connection
  } catch (err) {
    console.error('Failed to connect to Redis:', err);
  }
})();

// Handle Redis connection errors
redis.on('error', (err) => console.error('Redis Client Error:', err));

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

// Function to ingest data from Redis stream into PostgreSQL
async function ingestDataFromRedis() {
  const streamName = 'iot-data';
  let lastId = '0'; // Start from the beginning

  console.log('Started Redis ingestion process');

  while (true) {
    try {
      // Read multiple messages from Redis stream
      const messages = await redis.xRead(
        { key: streamName, id: lastId },
        { BLOCK: 5000, COUNT: 10 } // Block for 5 seconds or until 10 messages arrive
      );

      if (messages && messages.length > 0) {
        for (const message of messages) {
          const streamName = message.name;
          
          for (const entry of message.messages) {
            const id = entry.id;
            const data = entry.message;

            const timestamp = data.timestamp;
            const value = Number(data.value);

            const ioTData: IoTData = { timestamp, value };

            console.log(`Data from Redis: ${JSON.stringify(ioTData)}`);

            if (isValidData(ioTData)) {
              await prisma.iotData.create({
                data: {
                  timestamp: new Date(ioTData.timestamp),
                  value: ioTData.value,
                },
              });
              console.log('Data saved to PostgreSQL:', ioTData);

              // Update lastId and delete processed message
              lastId = id;
              await redis.xDel(streamName, id);
              console.log(`Deleted message ID ${id} from stream ${streamName}`);
            } else {
              console.warn('Invalid data:', ioTData);
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

  client.on('message', async (message: Buffer | string) => {
    try {
      const parsedMessage = typeof message === 'string' ? message : message.toString();
      const data: IoTData = JSON.parse(parsedMessage);

      console.log(`Data from WebSocket: ${parsedMessage}`);

      if (isValidData(data)) {
        console.log('Storing data to Redis stream...');
        
        if (!redis.isOpen) {
          console.error('Redis is not connected. Reconnecting...');
          await redis.connect();
        }

        try {
          const result = await redis.xAdd('iot-data', '*', {
            timestamp: data.timestamp,
            value: data.value.toString(),
          });
          console.log('Redis XADD result:', result);
        } catch (error) {
          console.error('Error adding to Redis stream:', error);
        }

        console.log('Data added to Redis Stream:', data);
      } else {
        console.warn('Invalid data received:', data);
      }

      client.send('Message from server');
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });
});

// Gracefully handle server shutdown
process.on('SIGINT', async () => {
  console.log('Closing Redis and Prisma connections...');
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
});
