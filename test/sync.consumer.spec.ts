import { SyncConsumer } from '../src/consumers/sync/sync.consumer';
import { KafkaService } from '../src/infra/kafka.service';
import { DatabaseService } from '../src/infra/database.service';
import { OutboundValidatorService } from '../src/engine/outbound-validator.service';
import { SystemAAdapter } from '../src/adapters/system-a.adapter';
import { SystemBAdapter } from '../src/adapters/system-b.adapter';
import { RedisService } from '../src/infra/redis.service';
import { CircuitBreakerService } from '../src/infra/circuit-breaker.service';
import { KAFKA_TOPICS, PIPELINES } from '../src/common/constants';
import { SyncEventRow } from '../src/common/types';

describe('SyncConsumer', () => {
  let syncConsumer: SyncConsumer;
  let dbService: DatabaseService;
  let kafkaService: KafkaService;
  let systemBAdapter: SystemBAdapter;

  beforeEach(() => {
    kafkaService = new KafkaService();
    dbService = new DatabaseService();
    const redisService = new RedisService();
    const circuitBreaker = new CircuitBreakerService();

    const outboundValidator = new OutboundValidatorService(dbService);
    const systemAAdapter = new SystemAAdapter(dbService);
    systemBAdapter = new SystemBAdapter(redisService, circuitBreaker, dbService);

    syncConsumer = new SyncConsumer(kafkaService, dbService, outboundValidator, systemAAdapter, systemBAdapter);
  });

  it('should claim PENDING records, execute destination adapter, and mark SUCCESS', async () => {
    // 1. Setup metadata
    await dbService.query(
      `INSERT INTO sync_metadata (tenant_id, pipeline_id, record_id, field_state)
       VALUES ($1, $2, $3, $4)`,
      [
        'tenant-1',
        PIPELINES.PIPELINES_1,
        'rec-sync-1',
        JSON.stringify({ name: { ts: 1000, source: 'system_a', event_id: 'ev-1' } }),
      ],
    );

    // 2. Stage pending record
    await dbService.query(
      `INSERT INTO sync_events (event_id, tenant_id, pipeline_id, record_id, destination_id, operation, payload, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        'ev-1',
        'tenant-1',
        PIPELINES.PIPELINES_1,
        'rec-sync-1',
        'system_b',
        'INSERT',
        { FullName: 'Alan Turing' },
        'PENDING',
      ],
    );

    // 3. Trigger scheduled job
    const claimedCount = await syncConsumer.handleScheduledJob({
      pipeline_id: PIPELINES.PIPELINES_1,
      timestamp: new Date().toISOString(),
    });

    expect(claimedCount).toBe(1);

    const check = await dbService.query(`SELECT status FROM sync_events WHERE event_id = $1`, ['ev-1']);
    expect(check.rows[0].status).toBe('SUCCESS');
  });

  it('should route to DLQ when max retries are exhausted on failure', async () => {
    systemBAdapter.force500Error = true;

    let dlqEmitted = false;
    jest.spyOn(kafkaService, 'produce').mockImplementation(async (topic, key, payload) => {
      if (topic === KAFKA_TOPICS.SYNC_EVENTS_DLQ) {
        dlqEmitted = true;
      }
    });

    const record: SyncEventRow = {
      id: 'event-fail-id',
      event_id: 'ev-exhausted',
      tenant_id: 'tenant-1',
      pipeline_id: PIPELINES.PIPELINES_1,
      record_id: 'rec-fail-1',
      destination_id: 'system_b',
      operation: 'UPDATE',
      payload: { Phone: '123' },
      status: 'PROCESSING',
      attempt_count: 2, // Next will be 3 (max_retries)
      max_retries: 3,
    };

    dbService.memorySyncEvents.set('event-fail-id', record);

    await syncConsumer.processRecord(record);

    expect(record.status).toBe('FAILED');
    expect(dlqEmitted).toBe(true);

    systemBAdapter.force500Error = false;
  });
});
