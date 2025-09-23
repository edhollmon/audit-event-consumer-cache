import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import 'dotenv/config';

const clientId = 'audit-event-consumer-cache'
const brokers = ['localhost:9092']
const groupId = 'audit-event-cache';
const topic = 'audit-event-topic';
const cacheKeyPrefix = 'events:tenant:'

// Setup Redis
const client = createClient();
client.on('error', err => console.log('Redis Client Error', err));
await client.connect();

// Kafka Setup
const kafka = new Kafka({
  clientId,
  brokers,
})

const consumer = kafka.consumer({
    groupId
})


await consumer.connect()
await consumer.subscribe({ topic, fromBeginning: true })


await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value.toString())
    // log event
    console.log("Event consumed to be cached") 
    console.log(event)

    // Send to cache
    const key = `${cacheKeyPrefix}${event?.tenantId}`
    client.rPush(key, JSON.stringify(event));
    client.expire(key, 300)
  },
})