import { NormalizationConsumer } from '../src/consumers/normalization/normalization.consumer';
import { KafkaService } from '../src/infra/kafka.service';
import { SystemAAdapter } from '../src/adapters/system-a.adapter';
import { SystemBAdapter } from '../src/adapters/system-b.adapter';
import { DatabaseService } from '../src/infra/database.service';
import { RedisService } from '../src/infra/redis.service';
import { CircuitBreakerService } from '../src/infra/circuit-breaker.service';
import { KAFKA_TOPICS } from '../src/common/constants';
import { CanonicalEvent } from '../src/common/types';

describe('NormalizationConsumer', () => {
  let consumer: NormalizationConsumer;
  let kafkaService: KafkaService;
  let systemAAdapter: SystemAAdapter;
  let systemBAdapter: SystemBAdapter;

  beforeEach(() => {
    kafkaService = new KafkaService();
    const dbService = new DatabaseService();
    const redisService = new RedisService();
    const circuitBreaker = new CircuitBreakerService();

    systemAAdapter = new SystemAAdapter(dbService);
    systemBAdapter = new SystemBAdapter(redisService, circuitBreaker, dbService);

    consumer = new NormalizationConsumer(kafkaService, systemAAdapter, systemBAdapter);
  });

  it('should consume CDC event from System A, normalize, and emit canonical event to sync-events', async () => {
    let emittedTopic = '';
    let emittedMessage: CanonicalEvent | null = null;

    jest.spyOn(kafkaService, 'produce').mockImplementation(async (topic, key, payload) => {
      emittedTopic = topic;
      emittedMessage = payload;
    });

    const cdcEvent = {
      tenant_id: 'tenant-1',
      event_id: 'cdc-10',
      op: 'c',
      before: null,
      after: { id: 'rec-1', first_name: 'David', last_name: 'Bowie' },
      source: { ts_ms: Date.now(), table: 'system_a_accounts' },
    };

    await consumer.handleNormalization(cdcEvent);

    expect(emittedTopic).toBe(KAFKA_TOPICS.SYNC_EVENTS);
    expect(emittedMessage).toBeDefined();
    expect(emittedMessage!.record_id).toBe('rec-1');
    expect(emittedMessage!.source).toBe('system_a');
    expect(emittedMessage!.record.name).toBe('David Bowie');
  });

  it('should consume Webhook event from System B, normalize, and emit canonical event to sync-events', async () => {
    let emittedTopic = '';
    let emittedMessage: CanonicalEvent | null = null;

    jest.spyOn(kafkaService, 'produce').mockImplementation(async (topic, key, payload) => {
      emittedTopic = topic;
      emittedMessage = payload;
    });

    const webhookEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-2',
      event_id: 'wh-20',
      event_type: 'UPDATE',
      changed_fields: ['FullName'],
      payload: { Id: 'rec-2', FullName: 'Freddie Mercury' },
      timestamp: new Date().toISOString(),
    };

    await consumer.handleSystemBWebhook(webhookEvent);

    expect(emittedTopic).toBe(KAFKA_TOPICS.SYNC_EVENTS);
    expect(emittedMessage).toBeDefined();
    expect(emittedMessage!.record_id).toBe('rec-2');
    expect(emittedMessage!.source).toBe('system_b');
    expect(emittedMessage!.record.name).toBe('Freddie Mercury');
  });

  it('should normalize System A CDC event using enveloped { meta, data } schema', async () => {
    let emittedTopic = '';
    let emittedMessage: CanonicalEvent | null = null;

    jest.spyOn(kafkaService, 'produce').mockImplementation(async (topic, key, payload) => {
      emittedTopic = topic;
      emittedMessage = payload;
    });

    const envelopedCDC = {
      meta: {
        type: 'internal',
        pipeline_id: 'pipeline-1',
        tenant_id: 'tenant-123',
        event_id: 'cdc-event-001',
      },
      data: {
        op: 'u',
        before: {
          id: 'rec-101',
          first_name: 'John',
          last_name: 'Doe',
          email: 'john.doe@example.com',
          phone: '+1-555-0100',
          company: 'Acme Corp',
        },
        after: {
          id: 'rec-101',
          first_name: 'Jonathan',
          last_name: 'Doe',
          email: 'jonathan.doe@example.com',
          phone: '+1-555-0199',
          company: 'Acme Corp',
        },
        source: {
          ts_ms: 1793915600000,
          table: 'system_a_accounts',
        },
      },
    };

    await consumer.handleNormalization(envelopedCDC, KAFKA_TOPICS.RAW_CDC_SYSTEM_A);

    expect(emittedTopic).toBe(KAFKA_TOPICS.SYNC_EVENTS);
    expect(emittedMessage).toBeDefined();
    expect(emittedMessage!.tenant_id).toBe('tenant-123');
    expect(emittedMessage!.record_id).toBe('rec-101');
    expect(emittedMessage!.event_id).toBe('cdc-event-001');
    expect(emittedMessage!.source).toBe('system_a');
    expect(emittedMessage!.record.name).toBe('Jonathan Doe');
    expect(emittedMessage!.record.phone).toBe('+1-555-0199');
    expect(emittedMessage!.changed_fields).toContain('phone');
  });

  it('should normalize System B Webhook event using enveloped { meta, data } schema', async () => {
    let emittedTopic = '';
    let emittedMessage: CanonicalEvent | null = null;

    jest.spyOn(kafkaService, 'produce').mockImplementation(async (topic, key, payload) => {
      emittedTopic = topic;
      emittedMessage = payload;
    });

    const envelopedWebhook = {
      meta: {
        type: 'internal',
        pipeline_id: 'pipeline-1',
        tenant_id: 'tenant-123',
        event_id: 'crm-wh-001',
      },
      data: {
        event_type: 'UPDATE',
        changed_fields: ['Phone', 'FullName'],
        payload: {
          Id: 'rec-101',
          FullName: 'Jonathan Doe',
          Phone: '+1-555-9999',
          Email: 'jonathan.doe@example.com',
          AccountName: 'Acme Corp',
        },
        timestamp: '2026-11-06T03:30:00.000Z',
      },
    };

    await consumer.handleNormalization(envelopedWebhook, KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B);

    expect(emittedTopic).toBe(KAFKA_TOPICS.SYNC_EVENTS);
    expect(emittedMessage).toBeDefined();
    expect(emittedMessage!.tenant_id).toBe('tenant-123');
    expect(emittedMessage!.record_id).toBe('rec-101');
    expect(emittedMessage!.event_id).toBe('crm-wh-001');
    expect(emittedMessage!.source).toBe('system_b');
    expect(emittedMessage!.record.name).toBe('Jonathan Doe');
    expect(emittedMessage!.record.phone).toBe('+1-555-9999');
  });
});
