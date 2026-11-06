import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '../../infra/kafka.service';
import { SystemAAdapter } from '../../adapters/system-a.adapter';
import { SystemBAdapter } from '../../adapters/system-b.adapter';
import { KAFKA_TOPICS } from '../../common/constants';

@Injectable()
export class NormalizationConsumer implements OnModuleInit {
  private readonly logger = new Logger(NormalizationConsumer.name);

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly systemAAdapter: SystemAAdapter,
    private readonly systemBAdapter: SystemBAdapter,
  ) {}

  async onModuleInit() {
    // 1. Subscribe to System A Debezium CDC and System B Webhook raw topics
    await this.kafkaService.subscribe(
      [KAFKA_TOPICS.RAW_CDC_SYSTEM_A, KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B],
      'normalization-group',
      async ({ topic, message }) => {
        await this.handleNormalization(message, topic);
      },
    );

    this.logger.log('Normalization Consumer initialized for CDC and Webhook topics');
  }

  async handleNormalization(message: any, topic?: string): Promise<void> {
    let isSystemA = false;
    if (topic === KAFKA_TOPICS.RAW_CDC_SYSTEM_A) {
      isSystemA = true;
    } else if (topic === KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B) {
      isSystemA = false;
    } else if (message?.op !== undefined || message?.data?.op !== undefined || message?.source?.table) {
      isSystemA = true;
    } else if (message?.data?.event_type !== undefined || message?.event_type !== undefined) {
      isSystemA = false;
    } else if (message?.meta?.source === 'system_a' || message?.meta?.type?.toLowerCase() === 'system_a') {
      isSystemA = true;
    } else {
      isSystemA = false;
    }

    if (isSystemA) {
      await this.handleSystemACDC(message);
    } else {
      await this.handleSystemBWebhook(message);
    }
  }

  async handleSystemACDC(rawEvent: any): Promise<void> {
    try {
      const eventId = rawEvent?.meta?.event_id || rawEvent?.event_id;
      this.logger.log(`Normalizing System A CDC event: ${eventId}`);
      const canonicalEvent = this.systemAAdapter.toCanonical(rawEvent);

      // Route to canonical topic partitioned by hash(tenant_id + ":" + record_id)
      const partitionKey = `${canonicalEvent.tenant_id}:${canonicalEvent.record_id}`;
      await this.kafkaService.produce(KAFKA_TOPICS.SYNC_EVENTS, partitionKey, canonicalEvent);
    } catch (err: any) {
      this.logger.error(`Error normalizing System A CDC event: ${err.message}`);
    }
  }

  async handleSystemBWebhook(rawEvent: any): Promise<void> {
    try {
      const eventId = rawEvent?.meta?.event_id || rawEvent?.event_id;
      this.logger.log(`Normalizing System B Webhook event: ${eventId}`);
      const canonicalEvent = this.systemBAdapter.toCanonical(rawEvent);

      // Route to canonical topic partitioned by hash(tenant_id + ":" + record_id)
      const partitionKey = `${canonicalEvent.tenant_id}:${canonicalEvent.record_id}`;
      await this.kafkaService.produce(KAFKA_TOPICS.SYNC_EVENTS, partitionKey, canonicalEvent);
    } catch (err: any) {
      this.logger.error(`Error normalizing System B Webhook event: ${err.message}`);
    }
  }
}
