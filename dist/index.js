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
const redis_1 = require("redis"); // ESM import for Redis
const app = (0, express_1.default)();
const httpserver = app.listen(8080, () => {
    console.log('Server is running on port 8080');
});
// Initialize WebSocket server
const socket = new ws_1.WebSocketServer({ server: httpserver });
// Create Redis client and handle connection errors
const redis = (0, redis_1.createClient)();
redis.on('error', (err) => console.error('Redis Client Error', err));
(() => __awaiter(void 0, void 0, void 0, function* () {
    try {
        yield redis.connect();
        console.log('Connected to Redis');
    }
    catch (error) {
        console.error('Redis connection error:', error);
    }
}))();
// Express route handler
app.get('/', (req, res) => {
    res.send('Hi there! This is the Express server.');
});
// WebSocket connection handler
socket.on('connection', (client) => {
    client.on('error', console.error);
    // Handle incoming messages from clients
    client.on('message', (message) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            const parsedMessage = typeof message === 'string' ? message : message.toString(); // Convert to string if necessary
            const data = JSON.parse(parsedMessage); // Parse JSON
            if (isValidData(data)) {
                yield redis.xAdd('iot-data', '*', {
                    timestamp: data.timestamp,
                    value: data.value.toString(), // Convert value to string
                });
                console.log('Data added to Redis Stream:', data);
            }
            else {
                console.warn('Invalid data received:', data);
            }
            console.log(`Received data from client: ${parsedMessage}`);
        }
        catch (err) {
            console.error('Error processing message:', err);
        }
    }));
    // Send message to client
    client.send('Message from server');
});
// Simple data validation function
function isValidData(data) {
    return !!data.timestamp && !!data.value; // Ensure both fields are present
}
