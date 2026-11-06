import { Injectable, Logger } from '@nestjs/common';
import { PIPELINES } from '../common/constants';
import { sleep } from '../common/utils';
import { SystemBAdapter } from '../adapters/system-b.adapter';
import { CircuitBreakerService } from '../infra/circuit-breaker.service';
import { JobSchedulerService } from '../scheduler/job-scheduler.service';
import { DebeziumCDCEvent } from '../schemas/system-a.schema';
import { CRMWebhookEvent } from '../schemas/system-b.schema';
import { EventEmitService } from './event-emit.service';
import { EventQueryService } from './event-query.service';

export type ScenarioName =
  | 'independent-concurrent'
  | 'field-conflict-stale'
  | 'echo-suppression-a-to-b'
  | 'rate-limiting-429'
  | 'circuit-breaker';

@Injectable()
export class ScenarioService {
  private readonly logger = new Logger(ScenarioService.name);

  constructor(
    private readonly eventEmitService: EventEmitService,
    private readonly eventQueryService: EventQueryService,
    private readonly jobScheduler: JobSchedulerService,
    private readonly circuitBreaker: CircuitBreakerService,
    private readonly systemBAdapter: SystemBAdapter,
  ) {}

  async runScenario(scenarioName: string): Promise<any> {
    this.logger.log(`Running automated orchestration scenario: ${scenarioName}`);
    const tenantId = 'tenant-demo';
    const recordId = `rec-${Date.now()}`;

    switch (scenarioName) {
      case 'independent-concurrent':
        return this.runIndependentConcurrent(tenantId, recordId);

      case 'field-conflict-stale':
        return this.runFieldConflictStale(tenantId, recordId);

      case 'echo-suppression-a-to-b':
        return this.runEchoSuppression(tenantId, recordId);

      case 'rate-limiting-429':
        return this.runRateLimiting(tenantId, recordId);

      case 'circuit-breaker':
        return this.runCircuitBreaker(tenantId, recordId);

      default:
        return {
          error: `Unknown scenario: ${scenarioName}. Available scenarios: independent-concurrent, field-conflict-stale, echo-suppression-a-to-b, rate-limiting-429, circuit-breaker`,
        };
    }
  }

  /**
   * Scenario: System A updates name; System B updates phone concurrently.
   * Both are accepted via per-field Last-Write-Wins conflict resolution.
   */
  private async runIndependentConcurrent(tenantId: string, recordId: string) {
    const eventA: DebeziumCDCEvent = {
      tenant_id: tenantId,
      event_id: `A-${Date.now()}`,
      op: 'u',
      before: { id: recordId, first_name: 'John', last_name: 'Doe', phone: '1111111111' },
      after: { id: recordId, first_name: 'Jonathan', last_name: 'Doe', phone: '1111111111' },
      source: { ts_ms: Date.now() - 100, table: 'system_a_accounts' },
    };
    const eventB: CRMWebhookEvent = {
      tenant_id: tenantId,
      record_id: recordId,
      event_id: `B-${Date.now()}`,
      event_type: 'UPDATE',
      changed_fields: ['Phone'],
      payload: { Id: recordId, Phone: '9999999999' },
      timestamp: new Date().toISOString(),
    };

    await this.eventEmitService.emitBoth({ system_a_event: eventA, system_b_event: eventB });
    await sleep(200);
    await this.jobScheduler.triggerNow(PIPELINES.PIPELINES_1);
    await sleep(200);

    const metadataResult = await this.eventQueryService.getSyncMetadata(recordId, tenantId, PIPELINES.PIPELINES_1);

    return {
      scenario: 'independent-concurrent',
      description: 'Independent updates to different fields (name vs phone) are both accepted via per-field LWW',
      recordId,
      metadataPipeline: metadataResult.metadata,
    };
  }

  /**
   * Scenario: Newer update arrives first; older stale update for the same
   * field is subsequently rejected by LWW conflict resolver.
   */
  private async runFieldConflictStale(tenantId: string, recordId: string) {
    const now = Date.now();

    const eventNew: DebeziumCDCEvent = {
      tenant_id: tenantId,
      event_id: `A-new-${now}`,
      op: 'u',
      before: { id: recordId, phone: '111' },
      after: { id: recordId, phone: '222' },
      source: { ts_ms: now, table: 'system_a_accounts' },
    };
    await this.eventEmitService.emitSystemA({ event: eventNew });
    await sleep(150);

    const eventOld: CRMWebhookEvent = {
      tenant_id: tenantId,
      record_id: recordId,
      event_id: `B-old-${now}`,
      event_type: 'UPDATE',
      changed_fields: ['Phone'],
      payload: { Id: recordId, Phone: '000' },
      timestamp: new Date(now - 60000).toISOString(), // 1 minute older
    };
    await this.eventEmitService.emitSystemB({ event: eventOld });
    await sleep(150);

    const metadata = await this.eventQueryService.getSyncMetadata(recordId, tenantId, PIPELINES.PIPELINES_1);
    return {
      scenario: 'field-conflict-stale',
      description: 'Older timestamp update for the same field was rejected as stale',
      recordId,
      metadata: metadata.metadata,
    };
  }

  /**
   * Scenario: System A event propagates to System B CRM; CRM echoes the
   * write back as a webhook which is detected via __sync_event_id and suppressed.
   */
  private async runEchoSuppression(tenantId: string, recordId: string) {
    const eventId = `A-orig-${Date.now()}`;

    const eventA: DebeziumCDCEvent = {
      tenant_id: tenantId,
      event_id: eventId,
      op: 'c',
      before: null,
      after: { id: recordId, first_name: 'Echo', last_name: 'Test', email: 'echo@test.com' },
      source: { ts_ms: Date.now(), table: 'system_a_accounts' },
    };
    await this.eventEmitService.emitSystemA({ event: eventA });
    await sleep(150);

    await this.jobScheduler.triggerNow(PIPELINES.PIPELINES_1);
    await sleep(150);

    const echoEventB: CRMWebhookEvent = {
      tenant_id: tenantId,
      record_id: recordId,
      event_id: `B-echo-${Date.now()}`,
      event_type: 'UPDATE',
      changed_fields: ['FullName'],
      payload: { Id: recordId, FullName: 'Echo Test', __sync_event_id: eventId },
      timestamp: new Date().toISOString(),
    };
    await this.eventEmitService.emitSystemB({ event: echoEventB });
    await sleep(150);

    const syncEvents = await this.eventQueryService.getSyncEvents(PIPELINES.PIPELINES_1, undefined, recordId);
    return {
      scenario: 'echo-suppression-a-to-b',
      description: 'CRM webhook containing __sync_event_id was identified as echo and suppressed',
      recordId,
      eventId,
      syncEvents: syncEvents.events,
    };
  }

  /**
   * Scenario: System B returns HTTP 429 (rate limit). The event stays PENDING
   * with exponential backoff and retry count incremented.
   */
  private async runRateLimiting(tenantId: string, recordId: string) {
    this.systemBAdapter.force429Error = true;

    const eventA: DebeziumCDCEvent = {
      tenant_id: tenantId,
      event_id: `A-rate-${Date.now()}`,
      op: 'u',
      before: { id: recordId, company: 'OldCo' },
      after: { id: recordId, company: 'BurstCo' },
      source: { ts_ms: Date.now(), table: 'system_a_accounts' },
    };
    await this.eventEmitService.emitSystemA({ event: eventA });
    await sleep(150);

    await this.jobScheduler.triggerNow(PIPELINES.PIPELINES_1);
    await sleep(150);

    this.systemBAdapter.force429Error = false;

    const syncEvents = await this.eventQueryService.getSyncEvents(PIPELINES.PIPELINES_1, undefined, recordId);
    return {
      scenario: 'rate-limiting-429',
      description: 'System B returned HTTP 429. Event remained PENDING with backoff and retry count incremented',
      recordId,
      syncEvents: syncEvents.events,
    };
  }

  /**
   * Scenario: Repeated HTTP 500 failures trip the circuit breaker to OPEN state,
   * blocking further CRM dispatch attempts.
   */
  private async runCircuitBreaker(tenantId: string, recordId: string) {
    this.systemBAdapter.force500Error = true;

    for (let i = 0; i < 5; i++) {
      const recId = `${recordId}-${i}`;
      const ev: DebeziumCDCEvent = {
        tenant_id: tenantId,
        event_id: `A-fail-${Date.now()}-${i}`,
        op: 'c',
        before: null,
        after: { id: recId, first_name: 'Fail', last_name: 'Test' },
        source: { ts_ms: Date.now(), table: 'system_a_accounts' },
      };
      await this.eventEmitService.emitSystemA({ event: ev });
    }
    await sleep(200);

    await this.jobScheduler.triggerNow(PIPELINES.PIPELINES_1);
    await sleep(300);

    const breakerState = this.circuitBreaker.getState();
    this.systemBAdapter.force500Error = false;
    this.circuitBreaker.reset();

    return {
      scenario: 'circuit-breaker',
      description: 'Repeated 500 API failures tripped circuit breaker',
      circuitBreakerState: breakerState,
    };
  }
}
