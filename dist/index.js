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
// Create Redis client
const redis = (0, redis_1.createClient)({ url: 'redis://127.0.0.1:6379' });
(() => __awaiter(void 0, void 0, void 0, function* () {
    try {
        yield redis.connect();
        console.log('Redis client connected successfully');
        ingestDataFromRedis(); // Start background ingestion after connection
    }
    catch (err) {
        console.error('Failed to connect to Redis:', err);
    }
}))();
// Handle Redis connection errors
redis.on('error', (err) => console.error('Redis Client Error:', err));
// Create Prisma client
const prisma = new client_1.PrismaClient();
// Simple data validation function
function isValidData(data) {
    return !!data.timestamp && typeof data.value === 'number';
}
// Function to ingest data from Redis stream into PostgreSQL
function ingestDataFromRedis() {
    return __awaiter(this, void 0, void 0, function* () {
        const streamName = 'iot-data';
        let lastId = '0'; // Start from the beginning
        console.log('Started Redis ingestion process');
        while (true) {
            try {
                // Read multiple messages from Redis stream
                const messages = yield redis.xRead({ key: streamName, id: lastId }, { BLOCK: 5000, COUNT: 10 } // Block for 5 seconds or until 10 messages arrive
                );
                if (messages && messages.length > 0) {
                    for (const message of messages) {
                        const streamName = message.name;
                        for (const entry of message.messages) {
                            const id = entry.id;
                            const data = entry.message;
                            const timestamp = data.timestamp;
                            const value = Number(data.value);
                            const ioTData = { timestamp, value };
                            console.log(`Data from Redis: ${JSON.stringify(ioTData)}`);
                            if (isValidData(ioTData)) {
                                yield prisma.iotData.create({
                                    data: {
                                        timestamp: new Date(ioTData.timestamp),
                                        value: ioTData.value,
                                    },
                                });
                                console.log('Data saved to PostgreSQL:', ioTData);
                                // Update lastId and delete processed message
                                lastId = id;
                                yield redis.xDel(streamName, id);
                                console.log(`Deleted message ID ${id} from stream ${streamName}`);
                            }
                            else {
                                console.warn('Invalid data:', ioTData);
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
    client.on('message', (message) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            const parsedMessage = typeof message === 'string' ? message : message.toString();
            const data = JSON.parse(parsedMessage);
            console.log(`Data from WebSocket: ${parsedMessage}`);
            if (isValidData(data)) {
                console.log('Storing data to Redis stream...');
                if (!redis.isOpen) {
                    console.error('Redis is not connected. Reconnecting...');
                    yield redis.connect();
                }
                try {
                    const result = yield redis.xAdd('iot-data', '*', {
                        timestamp: data.timestamp,
                        value: data.value.toString(),
                    });
                    console.log('Redis XADD result:', result);
                }
                catch (error) {
                    console.error('Error adding to Redis stream:', error);
                }
                console.log('Data added to Redis Stream:', data);
            }
            else {
                console.warn('Invalid data received:', data);
            }
            client.send('Message from server');
        }
        catch (err) {
            console.error('Error processing message:', err);
        }
    }));
});
// Gracefully handle server shutdown
process.on('SIGINT', () => __awaiter(void 0, void 0, void 0, function* () {
    console.log('Closing Redis and Prisma connections...');
    yield redis.quit();
    yield prisma.$disconnect();
    process.exit(0);
}));
