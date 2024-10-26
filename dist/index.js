"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const ws_1 = require("ws");
const redis_1 = require("redis");
const client_1 = require("@prisma/client");
const app = (0, express_1.default)();
const httpserver = app.listen(8080, () => {
    console.log('Server is running on port 8080');
});
// Initialize WebSocket server
const socket = new ws_1.WebSocketServer({ server: httpserver });
// Create Redis client and handle connection errors
const redis = (0, redis_1.createClient)({ url: 'redis://127.0.0.1:6379' });
redis.on('error', (err) => console.error('Redis Client Error', err));
// Create Prisma client
const prisma = new client_1.PrismaClient();
// Simple data validation function
function isValidData(data) {
    return !!data.timestamp && typeof data.value === 'number';
}
(() => __awaiter(void 0, void 0, void 0, function* () {
    try {
        yield redis.connect();
        console.log('Connected to Redis');
        // Start ingesting Redis stream data into PostgreSQL
        yield ingestDataFromRedis();
    }
    catch (error) {
        console.error('Redis connection error:', error);
    }
}))();
// Function to ingest data from Redis stream into PostgreSQL
function ingestDataFromRedis() {
    return __awaiter(this, void 0, void 0, function* () {
        const streamName = 'iot-data';
        let lastId = '0'; // Start reading from the beginning
        while (true) {
            try {
                // Read messages from Redis stream
                const messages = yield redis.xRead({ key: streamName, id: lastId }, { BLOCK: 0, COUNT: 1 } // Block until new messages arrive
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
                            const ioTData = {
                                timestamp: timestamp,
                                value: value,
                            };
                            // Log the entire flow
                            console.log(`Data coming from Redis: ${JSON.stringify(ioTData)}`);
                            // Validate and store in PostgreSQL
                            if (isValidData(ioTData)) {
                                yield prisma.iotData.create({
                                    data: {
                                        timestamp: new Date(ioTData.timestamp), // Ensure the timestamp is in a Date format
                                        value: ioTData.value,
                                    },
                                });
                                console.log('Data saved to PostgreSQL:', ioTData);
                                // Update lastId to mark the last processed message
                                lastId = id;
                                // Delete the message from the Redis stream after processing
                                yield redis.xDel(streamName, id); // Delete the processed message
                                console.log(`Deleted message with ID ${id} from stream ${streamName}`);
                            }
                            else {
                                console.warn('Invalid data received:', ioTData);
                            }
                        }
                    }
                }
            }
            catch (err) {
                console.error('Error reading from Redis stream:', err);
            }
        }
    });
}
// WebSocket connection handler
socket.on('connection', (client) => {
    console.log('WebSocket client connected');
    client.on('error', console.error);
    // Handle incoming messages from clients
    client.on('message', (message) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            const parsedMessage = typeof message === 'string' ? message : message.toString(); // Convert to string if necessary
            const data = JSON.parse(parsedMessage); // Parse JSON
            // Log the data received from the WebSocket client
            console.log(`Data coming from WebSocket: ${parsedMessage}`);
            // 1. Store data in Redis Stream
            console.log(data);
            yield redis.xAdd('iot-data', '*', {
                timestamp: data.timestamp,
                value: data.value.toString(),
            });
            console.log('Data added to Redis Stream:', data);
            {
                console.warn('Invalid data received from WebSocket:', data);
            }
            // Send a message to the client
            client.send('Message from server');
        }
        catch (err) {
            console.error('Error processing message:', err);
        }
    }));
});
