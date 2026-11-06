import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer, Consumer, Partitioners } from 'kafkajs';
import { hashPartition } from '../common/utils';

export type MessageHandler = (payload: { topic: string; partition: number; message: any }) => Promise<void>;

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);
  private kafka: Kafka | null = null;
  private producer: Producer | null = null;
  private isConnected: boolean = false;

  // In-memory fallback message bus for local tests and offline development
  private inMemoryListeners = new Map<string, MessageHandler[]>();

  async onModuleInit() {
    const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

    try {
      this.kafka = new Kafka({
        clientId: 'sync-engine',
        brokers,
        connectionTimeout: 1500,
        retry: { retries: 1 },
      });

      this.producer = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
      await this.producer.connect();
      this.isConnected = true;
      this.logger.log(`Connected to Kafka brokers at ${brokers.join(',')}`);
    } catch (err: any) {
      this.isConnected = false;
      this.logger.warn(`Kafka connection failed (${err.message}). Using resilient in-memory message bus.`);
    }
  }

  async onModuleDestroy() {
    if (this.producer && this.isConnected) {
      await this.producer.disconnect();
    }
  }

  isKafkaConnected(): boolean {
    return this.isConnected;
  }

  /**
   * Register a topic message handler (works for both live Kafka and in-memory bus)
   */
  async subscribe(topics: string[] | string, groupId: string, handler: MessageHandler): Promise<void> {
    const topicList = Array.isArray(topics) ? topics : [topics];
    for (const t of topicList) {
      const listeners = this.inMemoryListeners.get(t) || [];
      listeners.push(handler);
      this.inMemoryListeners.set(t, listeners);
    }

    if (this.kafka) {
      try {
        const consumer: Consumer = this.kafka.consumer({ groupId });
        await consumer.connect();
        await consumer.subscribe({ topics: topicList, fromBeginning: true });
        await consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            const parsed = message.value ? JSON.parse(message.value.toString()) : null;
            await handler({ topic, partition, message: parsed });
          },
        });
        this.logger.log(`Subscribed Kafka consumer group ${groupId} to topics: ${topics.toString()}`);
      } catch (err: any) {
        this.logger.warn(`Failed to start Kafka consumer for topics: ${topics.toString()}: ${err.message}`);
      }
    }
  }

  /**
   * Emit an event to a Kafka topic, partitioned by hash(tenant_id + ":" + record_id)
   */
  async produce(topic: string, key: string, payload: any): Promise<void> {
    const partition = hashPartition(key, 10);
    const valueString = JSON.stringify(payload);

    if (this.producer) {
      try {
        await this.producer.send({
          topic,
          messages: [{ key, value: valueString, partition }],
        });
        return;
      } catch (err: any) {
        this.logger.warn(`Failed to produce to Kafka topic ${topic}: ${err.message}. Using in-memory bus.`);
      }
    }

    // Deliver through in-memory bus
    const listeners = this.inMemoryListeners.get(topic) || [];
    for (const listener of listeners) {
      setImmediate(async () => {
        try {
          await listener({ topic, partition, message: payload });
        } catch (handlerErr: any) {
          this.logger.error(`Error in message handler for topic ${topic}: ${handlerErr.message}`);
        }
      });
    }
  }

  resetInMemory(): void {
    this.inMemoryListeners.clear();
  }
}
