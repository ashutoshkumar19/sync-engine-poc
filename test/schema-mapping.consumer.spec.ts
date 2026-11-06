import { SchemaMappingConsumer } from '../src/consumers/schema-mapping/schema-mapping.consumer';
import { KafkaService } from '../src/infra/kafka.service';
import { EchoDetectorService } from '../src/engine/echo-detector.service';
import { ConflictResolverService } from '../src/engine/conflict-resolver.service';
import { DatabaseService } from '../src/infra/database.service';
import { SystemAAdapter } from '../src/adapters/system-a.adapter';
import { SystemBAdapter } from '../src/adapters/system-b.adapter';
import { RedisService } from '../src/infra/redis.service';
import { CircuitBreakerService } from '../src/infra/circuit-breaker.service';
import { CanonicalEvent } from '../src/common/types';
import { PIPELINES } from '../src/common/constants';

describe('SchemaMappingConsumer', () => {
  let consumer: SchemaMappingConsumer;
  let dbService: DatabaseService;
  let echoDetector: EchoDetectorService;
  let conflictResolver: ConflictResolverService;
  let systemAAdapter: SystemAAdapter;
  let systemBAdapter: SystemBAdapter;

  beforeEach(() => {
    const kafkaService = new KafkaService();
    dbService = new DatabaseService();
    const redisService = new RedisService();
    const circuitBreaker = new CircuitBreakerService();

    echoDetector = new EchoDetectorService(dbService);
    conflictResolver = new ConflictResolverService(dbService);
    systemAAdapter = new SystemAAdapter(dbService);
    systemBAdapter = new SystemBAdapter(redisService, circuitBreaker, dbService);

    consumer = new SchemaMappingConsumer(
      kafkaService,
      echoDetector,
      conflictResolver,
      dbService,
      systemAAdapter,
      systemBAdapter,
    );
  });

  it('should process canonical event, resolve conflicts and stage in sync_events with status PENDING', async () => {
    const event: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-staging-1',
      event_id: 'ev-stage-1',
      source: 'system_a',
      source_timestamp: '2026-11-06T00:00:00.000Z',
      operation: 'INSERT',
      changed_fields: ['name', 'email'],
      record: { name: 'Ada Lovelace', email: 'ada@computing.org' },
      schema_version: 1,
      rule_version: 1,
    };

    await consumer.processCanonicalEvent(event);

    const res = await dbService.query(`SELECT * FROM sync_events WHERE pipeline_id = $1 AND record_id = $2`, [
      PIPELINES.PIPELINES_1,
      'rec-staging-1',
    ]);

    expect(res.rowCount).toBe(1);
    const row = res.rows[0];
    expect(row.status).toBe('PENDING');
    expect(row.payload.FullName).toBe('Ada Lovelace');
    expect(row.payload.Email).toBe('ada@computing.org');
    expect(row.payload.__sync_event_id).toBe('ev-stage-1');
  });

  it('should suppress echo and discard event when sync marker matches previous outbound operation', async () => {
    // 1. Stage prior outbound event
    await dbService.query(
      `INSERT INTO sync_events (event_id, tenant_id, pipeline_id, record_id, destination_id, operation, payload, status, echo_status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      ['orig-marker-99', 'tenant-1', PIPELINES.PIPELINES_1, 'rec-echo-1', 'system_b', 'UPDATE', {}, 'PENDING', null],
    );

    // 2. Incoming canonical event has matching sync marker
    const echoEvent: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-echo-1',
      event_id: 'crm-echo-event',
      source: 'system_b',
      source_timestamp: '2026-11-06T00:01:00.000Z',
      operation: 'UPDATE',
      changed_fields: ['name'],
      record: { name: 'Ada Lovelace' },
      schema_version: 1,
      rule_version: 1,
      sync_marker: 'orig-marker-99',
    };

    await consumer.processCanonicalEvent(echoEvent);

    // Verify it did not create a new pending operation in sync_events
    const check = await dbService.query(
      `SELECT * FROM sync_events WHERE pipeline_id = $1 AND record_id = $2 AND status = 'PENDING'`,
      [PIPELINES.PIPELINES_1, 'rec-echo-1'],
    );
    expect(check.rowCount).toBe(0);
  });
});
