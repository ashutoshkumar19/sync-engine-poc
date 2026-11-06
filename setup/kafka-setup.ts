/**
 * Setup Kafka topics used by Sync Engine consumers
 */

import { Kafka } from 'kafkajs';

interface TopicConfig {
  name: string;
  numPartitions?: number;
  replicationFactor?: number;
}

const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';

const topics: Array<TopicConfig> = [
  { name: 'cdc.system-a.events', numPartitions: 10 },
  { name: 'webhook.system-b.events', numPartitions: 10 },
  { name: 'sync-events', numPartitions: 10 },
  { name: 'scheduled-job', numPartitions: 10 },
];

async function createTopics() {
  const kafka = new Kafka({ clientId: 'sync-engine', brokers: KAFKA_BROKERS.split(',') });
  const admin = kafka.admin();

  await admin.connect();
  console.log(`Connected to Kafka brokers at ${KAFKA_BROKERS}`);

  for (const config of topics) {
    const { name, numPartitions } = config;
    try {
      await admin.createTopics({
        topics: [{ topic: name, numPartitions, replicationFactor: config.replicationFactor || 1 }],
      });
      console.log(`Created topic ${name} (partitions: ${numPartitions})`);
    } catch (err: any) {
      if (err.message?.includes(`Topic already exists`) || err.message?.includes('Not a leader')) {
        console.log(`Exists topic ${name}`);
      } else {
        console.error(`Error creating topic ${name}: ${err.message}`);
      }
    }
  }

  await admin.disconnect();
}

createTopics()
  .then(() => console.log('Topics created successfully'))
  .catch(console.error);
