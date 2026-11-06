import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '../../infra/kafka.service';
import { EchoDetectorService } from '../../engine/echo-detector.service';
import { ConflictResolverService } from '../../engine/conflict-resolver.service';
import { DatabaseService } from '../../infra/database.service';
import { SystemAAdapter } from '../../adapters/system-a.adapter';
import { SystemBAdapter } from '../../adapters/system-b.adapter';
import { CanonicalEvent, CanonicalRecord } from '../../common/types';
import { KAFKA_TOPICS, PIPELINES, SYSTEM_IDS } from '../../common/constants';

@Injectable()
export class SchemaMappingConsumer implements OnModuleInit {
  private readonly logger = new Logger(SchemaMappingConsumer.name);

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly echoDetector: EchoDetectorService,
    private readonly conflictResolver: ConflictResolverService,
    private readonly databaseService: DatabaseService,
    private readonly systemAAdapter: SystemAAdapter,
    private readonly systemBAdapter: SystemBAdapter,
  ) {}

  async onModuleInit() {
    await this.kafkaService.subscribe([KAFKA_TOPICS.SYNC_EVENTS], 'schema-mapping-group', async ({ message }) => {
      await this.processCanonicalEvent(message as CanonicalEvent);
    });

    this.logger.log('Schema Mapping Consumer initialized for canonical sync-events topic');
  }

  async processCanonicalEvent(event: CanonicalEvent): Promise<void> {
    if (!event || !event.record_id) {
      return;
    }

    this.logger.log(`Processing canonical event ${event.event_id} for record ${event.record_id} from ${event.source}`);

    // 1. Resolve pipeline & destination
    const isFromA = event.source === SYSTEM_IDS.SYSTEM_A;
    const pipelineId = PIPELINES.PIPELINES_1;
    const destinationId = isFromA ? SYSTEM_IDS.SYSTEM_B : SYSTEM_IDS.SYSTEM_A;
    const destinationAdapter = isFromA ? this.systemBAdapter : this.systemAAdapter;

    // 2. Echo Detection & Suppression
    if (event.sync_marker) {
      const echoCheck = await this.echoDetector.checkAndResolveEcho(event.sync_marker);
      if (echoCheck.isEcho) {
        this.logger.warn(`Echo detected for marker ${event.sync_marker}. Suppressing event ${event.event_id}.`);
        return; // Discard event as echo
      }
    }

    // 3. Atomic Conflict Resolution (Per-field LWW in PostgreSQL)
    const conflictResult = await this.conflictResolver.resolveConflicts(pipelineId, event);

    if (conflictResult.isDuplicate) {
      this.logger.warn(`Duplicate event ${event.event_id} detected. Discarding.`);
      return;
    }

    if (conflictResult.isAllStale) {
      this.logger.warn(`All fields in event ${event.event_id} are stale. Discarding.`);
      return;
    }

    // 4. Map only accepted fields to destination schema
    const acceptedRecord: Partial<CanonicalRecord> = {};
    for (const field of conflictResult.acceptedFields) {
      if (event.record[field] !== undefined) {
        acceptedRecord[field] = event.record[field];
      }
    }

    const destinationPayload = destinationAdapter.toDestination(
      event.record_id,
      acceptedRecord,
      event.operation,
      event.event_id, // Pass event_id as correlationId for echo prevention
    );

    // 5. Store destination payload in sync_events durable staging table
    const insertSql = `
      INSERT INTO sync_events (
        event_id, tenant_id, pipeline_id, record_id, destination_id,
        operation, payload, status, echo_status, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
      ON CONFLICT (pipeline_id, event_id) DO NOTHING
    `;

    try {
      await this.databaseService.query(insertSql, [
        event.event_id,
        event.tenant_id,
        pipelineId,
        event.record_id,
        destinationId,
        event.operation,
        JSON.stringify(destinationPayload),
        'PENDING',
        null,
      ]);

      this.logger.log(
        `Created PENDING sync_events staging row for record ${event.record_id} (event: ${event.event_id})`,
      );
    } catch (err: any) {
      this.logger.error(`Failed to stage sync_events record: ${err.message}`);
    }
  }
}
