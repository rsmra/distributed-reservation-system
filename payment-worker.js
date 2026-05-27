const { Kafka } = require("kafkajs");
const { Pool } = require("pg");
const Redis = require("ioredis");
const dotenv = require("dotenv");

dotenv.config();

const redis = new Redis({
    host: "localhost",
    port: 6379
});

const dbPool = new Pool({
    host: "localhost",
    port: 5000,
    user: "postgres",
    password: "password",
    database: "postgres"
});

const kafka = new Kafka({
    clientId: 'payment-worker-service',
    brokers: ["localhost:9092"]
});

// set up a function that simulates payments
async function simulatePayment(ticketData) {
    console.log(`Processing payment for ticket with ticketId ${ticketData}`);

    return new Promise((resolve, reject) => {

        setTimeout(() => {
            const isSuccess = Math.random() < 0.8;
            if (isSuccess) {
                console.log(`Payment is successful.`);
                resolve();
            } else {
                console.log(`Payment is unsuccessful.`);
                reject(new Error(`Credit card declined.`));
            }
        }, 2000);
        
    });
}

const consumer = kafka.consumer({groupId: `payment-processing-group`});

async function startWorker() {
    try {
        await consumer.connect();
        
        console.log(`Consumer connected!`);
        
        await consumer.subscribe({ topic: 'ticket.reserved', fromBeginning: true });

        await consumer.run({
            eachMessage: async ({topic, partition, message}) => {
                const messageData = JSON.parse(message.value.toString());
                const {idempotencyKey, ticketId } = messageData;

                try {
                    await simulatePayment(ticketId);
    
                    // on success, save to database
                    const queryString = "INSERT INTO orders (idempotency_key, ticket_id, status, created_at) VALUES ($1, $2, $3, NOW()) ON CONFLICT (idempotency_key) DO NOTHING";
                    await dbPool.query(queryString, [idempotencyKey, ticketId, 'PAID']);
                    console.log(`Success: Order details saved to database`);

                } catch (err) {
                    console.log(`Failure: Error while processing payment: ${err}`);
                    console.log(`Running compensating transactions...`);
                    const TICKET_KEY = 'inventory:vip_tickets'
                    await redis.incr(TICKET_KEY);
                    const IDEMPOTENCY_KEY = `idempotency:${idempotencyKey}`;
                    await redis.del(IDEMPOTENCY_KEY);
                    console.log(`Compensating transactions completed!`);
                }
            }
        })
    } catch (err) {
        console.log(`Worker crashed: ${err}`);
    }
}

startWorker();