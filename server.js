const express = require("express");
const dotenv = require("dotenv");
const Redis = require("ioredis");
const { Kafka } = require("kafkajs");

dotenv.config();

const app = express();

// set up redis instance
const redis = new Redis({
    host: '127.0.0.1',
    port: 6379
});

// set up Kafka
const kafka = new Kafka({
    clientId: 'flash-sale-gateway',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

// middlewares
app.use(express.json());


redis.on('connect', () => {
    console.log(`Connected to Redis!`);
})

// initiliase our inventory when the server starts
async function initiliaseInventory() {
    const TICKET_KEY = 'inventory:vip_tickets';

    const exists = await redis.exists(TICKET_KEY);

    if (!exists) {
        // set the total tickets to 1000
        await redis.set(TICKET_KEY, 1000);
        console.log(`Inventory initialised with 1000 VIP tickets.`);
    } else {
        const currentCount = await redis.get(TICKET_KEY);
        console.log(`Inventory already exists. Current tickets: ${currentCount}`);
    }
}

app.post('/api/buy', async (req, res) => {
    // since we are buying, it should be an idempotent operation
    // get the idempotency key
    const idempotencyKey = req.headers['idempotency-key'];

    if (!idempotencyKey) {
        console.log(`Idempotency key is missing.`);
        return res.status(400).json({
            status: "Error",
            message: "Idempotency key is missing. Idempotency-Key header is required."
        });
    }

    try {
        // idempotency check
        const IDEMPOTENCY_KEY = `idempotency:${idempotencyKey}`
        const idempotencyKeyExists = await redis.exists(IDEMPOTENCY_KEY);
        if (idempotencyKeyExists) {
            // duplicate request. send the cahced response to the user
            console.log(`Idempotency hit! Returning cached response to user.`);
            const cachedResponse = await redis.get(IDEMPOTENCY_KEY);
            return res.status(200).json(JSON.parse(cachedResponse));
        }

        // request is a first time request to buy. decrement ticket count in atomic operation
        const TICKET_KEY = `inventory:vip_tickets`
        const remainingTickets = await redis.decr(TICKET_KEY);

        if (remainingTickets < 0) {
            // it means remaining tickets was already 0 before this request
            await redis.incr(TICKET_KEY);
            return res.status(409).json({
                status: "Failed",
                message: "Tickets are completely sold out."
            });
        }

        // it means tickets are remaining and user can buy a ticket
        // decrement count of tickets in redis in atomic operation
        // CORRECTION: already decremented ticket count in line 74. No need to decrement double.
        // await redis.decr(TICKET_KEY);
        
        const responsePayload = {
            status: "Success",
            message: "Ticket reserved. You have 5 minutes to complete payment.",
            ticketId: 1000 - remainingTickets
        }

        // cache the response with the idempotency key to prevent double charging
        await redis.set(IDEMPOTENCY_KEY, JSON.stringify(responsePayload), 'EX', 86400);

        console.log(`Ticket reserved. Remaining tickets: ${remainingTickets}`);

        try {
            // push a message to Kafka topic tickets
            await producer.send({
                topic: 'ticket.reserved',
                messages: [
                    {
                        // retries for this exact order will go to the same partition
                        key: idempotencyKey,
                        value: JSON.stringify({
                            idempotencyKey: idempotencyKey,
                            ticketId: responsePayload.ticketId,
                            status: 'PAYMENT_PENDING',
                            timestamp: new Date().toISOString()
                        })
                    }
                ]
            });

        } catch (err) {
            console.error(`Error while publishing to Kafka. Executing compensating actions.`);

            await redis.incr(TICKET_KEY);
            await redis.del(`idempotency:${idempotencyKey}`);

            throw new Error(`Kafka is unavailable, rolled back reservation.`);
        }


        console.log(`Published [ticket.reserved] event to Kafka for ${responsePayload.ticketId}`);

        return res.status(200).json(responsePayload);

    } catch (err) {
        console.error(`Server error during checkout: ${err}`);
        return res.status(500).json({
            status: "Error",
            message: "Internal Server Error during checkout."
        })
    }
});

// connect kafka producer
async function connectKafkaProducer() {
    try {
        await producer.connect();
        console.log('Kafka producer connected!');
    } catch (err) {
        console.error('Error while trying to connect Kafka producer.');
    }
}


async function startServer() {
    try {
        await connectKafkaProducer();
        console.log('Kafka Producer connected...');
        await initiliaseInventory();
        console.log('Inventory initialised...');
        app.listen(3000, async () => {
            console.log(`Gateway server running on port 3000...`);
        });
    } catch (err) {
        console.error(`Error while starting server: ${err}`);
        process.exit(1);
    }
}

startServer();
