import { DemoController } from '../src/controllers/demo.controller';
import { KafkaService } from '../src/infra/kafka.service';
import { DatabaseService } from '../src/infra/database.service';
import { RedisService } from '../src/infra/redis.service';
import { CircuitBreakerService } from '../src/infra/circuit-breaker.service';
import { JobSchedulerService } from '../src/scheduler/job-scheduler.service';
import { SystemAAdapter } from '../src/adapters/system-a.adapter';
import { SystemBAdapter } from '../src/adapters/system-b.adapter';
import { EchoDetectorService } from '../src/engine/echo-detector.service';
import { ConflictResolverService } from '../src/engine/conflict-resolver.service';
import { OutboundValidatorService } from '../src/engine/outbound-validator.service';
import { NormalizationConsumer } from '../src/consumers/normalization/normalization.consumer';
import { SchemaMappingConsumer } from '../src/consumers/schema-mapping/schema-mapping.consumer';
import { SyncConsumer } from '../src/consumers/sync/sync.consumer';
import { EventEmitService } from '../src/services/event-emit.service';
import { EventQueryService } from '../src/services/event-query.service';
import { SystemStatusService } from '../src/services/system-status.service';
import { ScenarioService } from '../src/services/scenario.service';
import { sleep } from '../src/common/utils';
import { PIPELINES } from '../src/common/constants';

describe('End-to-End Synchronization Scenarios', () => {
  let controller: DemoController;
  let dbService: DatabaseService;
  let kafkaService: KafkaService;
  let scheduler: JobSchedulerService;
  let normalizationConsumer: NormalizationConsumer;
  let schemaMappingConsumer: SchemaMappingConsumer;
  let syncConsumer: SyncConsumer;

  beforeEach(async () => {
    kafkaService = new KafkaService();
    dbService = new DatabaseService();
    const redisService = new RedisService();
    const circuitBreaker = new CircuitBreakerService();

    const systemAAdapter = new SystemAAdapter(dbService);
    const systemBAdapter = new SystemBAdapter(redisService, circuitBreaker, dbService);

    const echoDetector = new EchoDetectorService(dbService);
    const conflictResolver = new ConflictResolverService(dbService);
    const outboundValidator = new OutboundValidatorService(dbService);

    normalizationConsumer = new NormalizationConsumer(kafkaService, systemAAdapter, systemBAdapter);
    schemaMappingConsumer = new SchemaMappingConsumer(
      kafkaService,
      echoDetector,
      conflictResolver,
      dbService,
      systemAAdapter,
      systemBAdapter,
    );
    syncConsumer = new SyncConsumer(kafkaService, dbService, outboundValidator, systemAAdapter, systemBAdapter);
    scheduler = new JobSchedulerService(kafkaService);

    // Initialize module listeners
    await normalizationConsumer.onModuleInit();
    await schemaMappingConsumer.onModuleInit();
    await syncConsumer.onModuleInit();

    // Instantiate services and wire controller
    const eventEmitService = new EventEmitService(kafkaService);
    const eventQueryService = new EventQueryService(dbService, systemAAdapter, systemBAdapter);
    const systemStatusService = new SystemStatusService(
      dbService,
      kafkaService,
      redisService,
      circuitBreaker,
      scheduler,
    );
    const scenarioService = new ScenarioService(
      eventEmitService,
      eventQueryService,
      scheduler,
      circuitBreaker,
      systemBAdapter,
    );

    controller = new DemoController(
      eventEmitService,
      eventQueryService,
      systemStatusService,
      scenarioService,
      scheduler,
    );
  });

  it('Scenario 1: End-to-end sync from System A (Postgres CDC) to System B (CRM)', async () => {
    const recordId = 'rec-e2e-a2b';
    const cdcEvent = {
      tenant_id: 'tenant-demo',
      event_id: 'A-e2e-1',
      op: 'c' as const,
      before: null,
      after: {
        id: recordId,
        first_name: 'Grace',
        last_name: 'Hopper',
        email: 'grace@navy.mil',
        phone: '1010101010',
        company: 'US Navy',
      },
      source: { ts_ms: Date.now(), table: 'system_a_accounts' },
    };

    // 1. Emit CDC event
    await controller.emitSystemA({ event: cdcEvent });
    await sleep(50); // Allow normalization and schema mapping to process

    // 2. Verify record is staged in sync_events with status PENDING
    const staged = await controller.getSyncEvents(PIPELINES.PIPELINES_1, 'PENDING', recordId);
    expect(staged.count).toBe(1);

    // 3. Trigger Job Scheduler to dispatch pending records
    await controller.triggerScheduler(PIPELINES.PIPELINES_1);
    await sleep(50); // Allow sync consumer to process

    // 4. Verify record in sync_events is marked SUCCESS
    const finished = await controller.getSyncEvents(PIPELINES.PIPELINES_1, 'SUCCESS', recordId);
    expect(finished.count).toBe(1);

    // 5. Verify record was written to System B CRM mock table
    const crmRecord = await controller.getSystemBRecord(recordId);
    expect(crmRecord.found).toBe(true);
    expect(crmRecord.record.full_name).toBe('Grace Hopper');
    expect(crmRecord.record.email).toBe('grace@navy.mil');
  });

  it('Scenario 2: Automated orchestration for independent concurrent updates', async () => {
    const res = await controller.runScenario('independent-concurrent');
    expect(res.scenario).toBe('independent-concurrent');
    expect(res.recordId).toBeDefined();
  });

  it('Scenario 3: Automated orchestration for field conflict and stale rejection', async () => {
    const res = await controller.runScenario('field-conflict-stale');
    expect(res.scenario).toBe('field-conflict-stale');
  });

  it('Scenario 4: Automated orchestration for echo suppression', async () => {
    const res = await controller.runScenario('echo-suppression-a-to-b');
    expect(res.scenario).toBe('echo-suppression-a-to-b');
  });

  it('Scenario 5: Automated orchestration for rate limiting 429 and retry backoff', async () => {
    const res = await controller.runScenario('rate-limiting-429');
    expect(res.scenario).toBe('rate-limiting-429');
  });

  it('Scenario 6: Automated orchestration for circuit breaker tripping', async () => {
    const res = await controller.runScenario('circuit-breaker');
    expect(res.scenario).toBe('circuit-breaker');
    expect(res.circuitBreakerState.state).toBe('OPEN');
  });
});
