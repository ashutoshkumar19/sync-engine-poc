import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '../../infra/kafka.service';
import { DatabaseService } from '../../infra/database.service';
import { OutboundValidatorService } from '../../engine/outbound-validator.service';
import { SystemAAdapter } from '../../adapters/system-a.adapter';
import { SystemBAdapter } from '../../adapters/system-b.adapter';
import { ScheduledJobPayload, SyncEventRow } from '../../common/types';
import { KAFKA_TOPICS, RATE_LIMIT_CONFIG, SCHEDULER_CONFIG, SYSTEM_IDS } from '../../common/constants';

@Injectable()
export class SyncConsumer implements OnModuleInit {
  private readonly logger = new Logger(SyncConsumer.name);

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly databaseService: DatabaseService,
    private readonly outboundValidator: OutboundValidatorService,
    private readonly systemAAdapter: SystemAAdapter,
    private readonly systemBAdapter: SystemBAdapter,
  ) {}

  async onModuleInit() {
    await this.kafkaService.subscribe([KAFKA_TOPICS.SCHEDULED_JOB], 'sync-engine-group', async ({ message }) => {
      await this.handleScheduledJob(message as ScheduledJobPayload);
    });

    this.logger.log('Sync Consumer initialized for scheduled-job topic');
  }

  async handleScheduledJob(job: ScheduledJobPayload): Promise<number> {
    const pipelineId = job?.pipeline_id;
    if (!pipelineId) return 0;

    this.logger.log(`Sync Consumer triggered for pipeline ${pipelineId}`);

    // 1. Fetch bounded batch of PENDING records for pipeline fairness
    const fetchSql = `
      SELECT id, event_id, tenant_id, pipeline_id, record_id, destination_id,
             operation, payload, status, attempt_count, max_retries
      FROM sync_events
      WHERE pipeline_id = $1 AND status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT $2
    `;
    const pendingRes = await this.databaseService.query<SyncEventRow>(fetchSql, [
      pipelineId,
      SCHEDULER_CONFIG.BATCH_SIZE,
    ]);

    if (!pendingRes.rows || pendingRes.rows.length === 0) {
      return 0;
    }

    const candidateIds = pendingRes.rows.map((r) => r.id!);

    // 2. Atomically claim records by updating status to PROCESSING
    const claimSql = `
      UPDATE sync_events
      SET status = 'PROCESSING', updated_at = NOW()
      WHERE id = ANY($1) AND status = 'PENDING'
      RETURNING id, event_id, tenant_id, pipeline_id, record_id, destination_id,
                operation, payload, status, attempt_count, max_retries
    `;
    const claimedRes = await this.databaseService.query<SyncEventRow>(claimSql, [candidateIds]);
    const claimedRows = claimedRes.rows || [];

    this.logger.log(`Claimed ${claimedRows.length} records for pipeline ${pipelineId}`);

    // 3. Process each claimed record
    for (const record of claimedRows) {
      await this.processRecord(record);
    }

    return claimedRows.length;
  }

  async processRecord(record: SyncEventRow): Promise<void> {
    const {
      id,
      event_id,
      tenant_id,
      record_id,
      destination_id,
      operation,
      attempt_count = 0,
      max_retries = 3,
    } = record;

    // A. Revalidate fields against latest sync_metadata before dispatch
    const validation = await this.outboundValidator.revalidateBeforeDispatch(record);

    if (validation.isAllSuperseded) {
      this.logger.warn(`Record ${record_id} (event: ${event_id}) is completely SUPERSEDED. Skipping dispatch.`);
      await this.databaseService.query(
        `UPDATE sync_events SET status = 'SUPERSEDED', updated_at = NOW() WHERE id = $1`,
        [id],
      );
      record.status = 'SUPERSEDED';
      return;
    }

    const payloadToSend = validation.filteredPayload;

    // B. Invoke appropriate destination adapter
    const adapter = destination_id === SYSTEM_IDS.SYSTEM_B ? this.systemBAdapter : this.systemAAdapter;
    const dispatchResult = await adapter.executeOperation(tenant_id, record_id, operation, payloadToSend);

    // C. Handle outcome
    if (dispatchResult.success) {
      this.logger.log(`Record ${record_id} sync SUCCESS (event: ${event_id})`);
      await this.databaseService.query(`UPDATE sync_events SET status = 'SUCCESS', updated_at = NOW() WHERE id = $1`, [
        id,
      ]);
      record.status = 'SUCCESS';
    } else {
      const isTransient =
        dispatchResult.statusCode === 429 || (dispatchResult.statusCode && dispatchResult.statusCode >= 500);
      const nextAttemptCount = attempt_count + 1;

      if (isTransient && nextAttemptCount < max_retries) {
        // Calculate backoff
        const backoffMs =
          dispatchResult.retryAfterMs || RATE_LIMIT_CONFIG.BACKOFF_BASE_MS * Math.pow(2, nextAttemptCount);
        const nextAttemptAt = new Date(Date.now() + backoffMs);

        this.logger.warn(
          `Transient error on record ${record_id} (${dispatchResult.error}). Retrying in ${backoffMs}ms (attempt ${nextAttemptCount}/${max_retries})`,
        );

        await this.databaseService.query(
          `UPDATE sync_events SET status = 'PENDING', attempt_count = $1, next_attempt_at = $2, error_message = $3, updated_at = NOW() WHERE id = $4`,
          [nextAttemptCount, nextAttemptAt, dispatchResult.error || 'Transient error', id],
        );
        record.status = 'PENDING';
        record.attempt_count = nextAttemptCount;
      } else {
        // Permanent failure or retries exhausted -> Mark FAILED & Route to DLQ
        this.logger.error(`Permanent failure for record ${record_id} (attempts: ${nextAttemptCount}). Routing to DLQ.`);

        await this.databaseService.query(
          `UPDATE sync_events SET status = 'FAILED', attempt_count = $1, error_message = $2, updated_at = NOW() WHERE id = $3`,
          [nextAttemptCount, dispatchResult.error || 'Max retries exhausted', id],
        );
        record.status = 'FAILED';
        record.attempt_count = nextAttemptCount;

        await this.kafkaService.produce(KAFKA_TOPICS.SYNC_EVENTS_DLQ, `${tenant_id}:${record_id}`, {
          sync_event_id: id,
          event_id,
          tenant_id,
          record_id,
          destination_id,
          error: dispatchResult.error,
          payload: payloadToSend,
          failed_at: new Date().toISOString(),
        });
      }
    }
  }
}
