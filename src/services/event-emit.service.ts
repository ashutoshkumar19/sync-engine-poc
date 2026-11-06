import { Injectable, Logger } from '@nestjs/common';
import { KAFKA_TOPICS, PIPELINES } from '../common/constants';
import { KafkaService } from '../infra/kafka.service';

export interface EmitSystemAResult {
  success: boolean;
  topic: string;
  partitionKey: string;
  eventId: string;
  message: string;
}

export interface EmitSystemBResult {
  success: boolean;
  topic: string;
  partitionKey: string;
  eventId: string;
  message: string;
}

export interface EmitBothResult {
  success: boolean;
  results: {
    system_a?: { emitted: boolean; topic: string; eventId: string };
    system_b?: { emitted: boolean; topic: string; eventId: string };
  };
  message: string;
}

export interface BurstOptions {
  count: number;
  systemA: boolean;
  systemB: boolean;
  tenantId: string;
  pipelineId: string;
}

export interface BurstResult {
  success: boolean;
  count: number;
  system_a_count: number;
  system_b_count: number;
  events: any[];
}

@Injectable()
export class EventEmitService {
  private readonly logger = new Logger(EventEmitService.name);

  // Name pools for realistic burst data
  private readonly FIRST_NAMES = ['Alice', 'Bob', 'Carol', 'David', 'Eve', 'Frank', 'Grace', 'Henry'];
  private readonly LAST_NAMES = ['Smith', 'Jones', 'Williams', 'Brown', 'Taylor', 'Wilson', 'Davis', 'Miller'];
  private readonly COMPANIES = ['Acme Corp', 'Globex', 'Initech', 'Umbrella Ltd', 'Hooli', 'Pied Piper'];
  private readonly SYS_B_CHANGED_FIELDS_OPTIONS = [
    ['FullName'],
    ['Phone'],
    ['Email'],
    ['FullName', 'Phone'],
    ['FullName', 'Email', 'Phone'],
  ];

  constructor(private readonly kafkaService: KafkaService) {}

  /**
   * Wraps a raw System A body into the enveloped { meta, data } format and
   * produces it to the RAW_CDC_SYSTEM_A Kafka topic.
   */
  async emitSystemA(body: any): Promise<EmitSystemAResult> {
    const meta = body?.meta || {};
    const data = body?.data || body?.event || body;

    const tenantId = meta.tenant_id || data.tenant_id || 'tenant-123';
    const eventId = meta.event_id || data.event_id || `A-${Date.now()}`;
    const recordId = data.after?.id || data.before?.id || data.record_id || data.id || 'default';

    const payload =
      body?.meta && body?.data
        ? body
        : {
            meta: {
              type: meta.type || 'internal',
              pipeline_id: meta.pipeline_id || PIPELINES.PIPELINES_1,
              tenant_id: tenantId,
              event_id: eventId,
            },
            data,
          };

    const key = `${tenantId}:${recordId}`;
    await this.kafkaService.produce(KAFKA_TOPICS.RAW_CDC_SYSTEM_A, key, payload);

    this.logger.log(`System A CDC event emitted: ${eventId} → topic=${KAFKA_TOPICS.RAW_CDC_SYSTEM_A}`);

    return {
      success: true,
      topic: KAFKA_TOPICS.RAW_CDC_SYSTEM_A,
      partitionKey: key,
      eventId,
      message: 'System A CDC event emitted to raw topic',
    };
  }

  /**
   * Wraps a raw System B body into the enveloped { meta, data } format and
   * produces it to the RAW_WEBHOOK_SYSTEM_B Kafka topic.
   */
  async emitSystemB(body: any): Promise<EmitSystemBResult> {
    const meta = body?.meta || {};
    const data = body?.data || body?.event || body;

    const tenantId = meta.tenant_id || data.tenant_id || 'tenant-123';
    const eventId = meta.event_id || data.event_id || `B-${Date.now()}`;
    const recordId = data.record_id || data.payload?.Id || data.payload?.id || data.Id || data.id || 'default';

    const payload =
      body?.meta && body?.data
        ? body
        : {
            meta: {
              type: meta.type || 'internal',
              pipeline_id: meta.pipeline_id || PIPELINES.PIPELINES_1,
              tenant_id: tenantId,
              event_id: eventId,
            },
            data,
          };

    const key = `${tenantId}:${recordId}`;
    await this.kafkaService.produce(KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B, key, payload);

    this.logger.log(`System B webhook event emitted: ${eventId} → topic=${KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B}`);

    return {
      success: true,
      topic: KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B,
      partitionKey: key,
      eventId,
      message: 'System B Webhook event emitted to raw topic',
    };
  }

  /**
   * Concurrently emits System A and System B events for a combined bidirectional
   * update scenario.
   */
  async emitBoth(body: any): Promise<EmitBothResult> {
    const results: EmitBothResult['results'] = {};
    const tasks: Promise<void>[] = [];

    if (body.system_a_event) {
      tasks.push(
        this.emitSystemA(body.system_a_event).then((r) => {
          results.system_a = { emitted: true, topic: r.topic, eventId: r.eventId };
        }),
      );
    }

    if (body.system_b_event) {
      tasks.push(
        this.emitSystemB(body.system_b_event).then((r) => {
          results.system_b = { emitted: true, topic: r.topic, eventId: r.eventId };
        }),
      );
    }

    await Promise.all(tasks);

    return {
      success: true,
      results,
      message: 'Combined events emitted to their respective raw topics',
    };
  }

  /**
   * Emits a configurable burst of System A CDC events and/or System B webhook
   * events in parallel using Promise.all.
   */
  async emitBurst(options: BurstOptions): Promise<BurstResult> {
    const { count, systemA, systemB, tenantId, pipelineId } = options;

    // Build all produce tasks upfront then fire all in parallel
    const emitTasks: Promise<{ channel: string; [k: string]: any }>[] = [];

    for (let i = 0; i < count; i++) {
      const timestamp = Date.now() + i; // increasing by 1ms per event

      if (systemA) {
        const eventId = `event-${timestamp}-${Math.floor(Math.random() * 10) + 1}`;
        const recordId = `record-${Math.floor(Math.random() * 10000) + 1}`;
        const op = i % 3 === 0 ? 'c' : 'u';
        const firstName = this.FIRST_NAMES[i % this.FIRST_NAMES.length];
        const lastName = this.LAST_NAMES[(i + 2) % this.LAST_NAMES.length];
        const company = this.COMPANIES[i % this.COMPANIES.length];
        const email = `${firstName.toLowerCase()}.${lastName.toLowerCase()}${i}@example.com`;
        const phone = `+1-${500 + (i % 400)}-${100 + (i % 900)}-${1000 + (i % 9000)}`;

        const beforeImage =
          op === 'c'
            ? null
            : {
                id: recordId,
                first_name: firstName,
                last_name: lastName,
                email: `${firstName.toLowerCase()}${i}@old.com`,
                phone: `+1-999-000-${1000 + i}`,
                company,
              };
        const afterImage = { id: recordId, first_name: firstName, last_name: lastName, email, phone, company };

        const systemAEvent = {
          meta: { type: 'internal', pipeline_id: pipelineId, tenant_id: tenantId, event_id: eventId },
          data: {
            op,
            before: beforeImage,
            after: afterImage,
            source: { ts_ms: timestamp, table: 'system_a_accounts' },
          },
        };

        emitTasks.push(
          this.kafkaService
            .produce(KAFKA_TOPICS.RAW_CDC_SYSTEM_A, `${tenantId}:${recordId}`, systemAEvent)
            .then(() => ({
              channel: 'system-a',
              eventId,
              recordId,
              op,
              timestamp: new Date(timestamp).toISOString(),
              after: afterImage,
              topic: KAFKA_TOPICS.RAW_CDC_SYSTEM_A,
            })),
        );
      }

      if (systemB) {
        const eventId = `event-${timestamp}-${Math.floor(Math.random() * 10) + 1}`;
        const eventType = i % 3 === 0 ? 'CREATE' : 'UPDATE';
        const crmRecordId = `record-${Math.floor(Math.random() * 10000) + 1}`;
        const fullName = `${this.FIRST_NAMES[(i + 1) % this.FIRST_NAMES.length]} ${this.LAST_NAMES[i % this.LAST_NAMES.length]}`;
        const email = `crm.${fullName.toLowerCase().replace(' ', '.')}${i}@example.com`;
        const phone = `+1-${600 + (i % 400)}-${200 + (i % 900)}-${2000 + (i % 9000)}`;
        const accountName = this.COMPANIES[(i + 1) % this.COMPANIES.length];
        const changedFields = this.SYS_B_CHANGED_FIELDS_OPTIONS[i % this.SYS_B_CHANGED_FIELDS_OPTIONS.length];
        const sysBEventId = `event-${eventId}`;

        const systemBEvent = {
          meta: { type: 'external', pipeline_id: pipelineId, tenant_id: tenantId, event_id: sysBEventId },
          data: {
            event_type: eventType,
            record_id: crmRecordId,
            changed_fields: changedFields,
            payload: { Id: crmRecordId, FullName: fullName, Email: email, Phone: phone, AccountName: accountName },
            timestamp: new Date(timestamp).toISOString(),
          },
        };

        emitTasks.push(
          this.kafkaService
            .produce(KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B, `${tenantId}:${crmRecordId}`, systemBEvent)
            .then(() => ({
              channel: 'system-b',
              eventId: sysBEventId,
              recordId: crmRecordId,
              eventType,
              changedFields,
              timestamp: new Date(timestamp).toISOString(),
              payload: systemBEvent.data.payload,
              topic: KAFKA_TOPICS.RAW_WEBHOOK_SYSTEM_B,
            })),
        );
      }
    }

    // Fire all System A and System B produce calls simultaneously
    const results = await Promise.all(emitTasks);

    return {
      success: true,
      count: results.length,
      system_a_count: results.filter((r) => r.channel === 'system-a').length,
      system_b_count: results.filter((r) => r.channel === 'system-b').length,
      events: results.slice(0, 5),
    };
  }
}
