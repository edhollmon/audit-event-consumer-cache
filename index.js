import { Kafka } from 'kafkajs';
import 'dotenv/config';

const clientId = 'audit-event-consumer-cache'
const brokers = ['localhost:9092']
const groupId = 'audit-event-cache';
const topic = 'audit-event-topic';

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
    
  },
})