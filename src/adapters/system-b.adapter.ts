import { Injectable, Logger } from '@nestjs/common';
import { ISystemAdapter } from './adapter.interface';
import { CanonicalEvent, CanonicalRecord, CRUDOperation, DestinationDispatchResult } from '../common/types';
import { CRMWebhookEvent, SystemBSchemaValidator } from '../schemas/system-b.schema';
import { RedisService } from '../infra/redis.service';
import { CircuitBreakerService } from '../infra/circuit-breaker.service';
import { DatabaseService } from '../infra/database.service';
import { RATE_LIMIT_CONFIG, SYSTEM_IDS } from '../common/constants';

@Injectable()
export class SystemBAdapter implements ISystemAdapter {
  private readonly logger = new Logger(SystemBAdapter.name);
  readonly systemId = SYSTEM_IDS.SYSTEM_B;

  // Simulation flags for demo and testing scenarios
  public force429Error: boolean = false;
  public force500Error: boolean = false;

  constructor(
    private readonly redisService: RedisService,
    private readonly circuitBreaker: CircuitBreakerService,
    private readonly databaseService: DatabaseService,
  ) {}

  toCanonical(sourceEvent: any): CanonicalEvent {
    const unwrapped = SystemBSchemaValidator.unwrapEvent(sourceEvent);
    const validation = SystemBSchemaValidator.validateWebhook(unwrapped);
    if (!validation.valid) {
      throw new Error(`Invalid CRM webhook event: ${validation.errors.join(', ')}`);
    }

    const { tenant_id, record_id, event_id, event_type, payload = {}, timestamp, changed_fields } = unwrapped;

    let operation: CRUDOperation;
    switch (event_type) {
      case 'CREATE':
        operation = 'INSERT';
        break;
      case 'DELETE':
        operation = 'DELETE';
        break;
      case 'UPDATE':
      default:
        operation = 'UPDATE';
        break;
    }

    // Map changed fields from CRM schema to canonical names
    const changed: string[] = [];
    const rawChanged = changed_fields || Object.keys(payload);
    for (const field of rawChanged) {
      const lower = field.toLowerCase();
      if (lower.includes('name') && !changed.includes('name')) changed.push('name');
      if (lower.includes('email') && !changed.includes('email')) changed.push('email');
      if (lower.includes('phone') && !changed.includes('phone')) changed.push('phone');
      if (lower.includes('account') && !changed.includes('company')) changed.push('company');
    }

    if (operation === 'DELETE') {
      changed.push('name', 'email', 'phone', 'company');
    }

    const canonicalRecord: CanonicalRecord = {};
    if (payload.FullName !== undefined) canonicalRecord.name = payload.FullName;
    if (payload.Email !== undefined) canonicalRecord.email = payload.Email;
    if (payload.Phone !== undefined) canonicalRecord.phone = payload.Phone;
    if (payload.AccountName !== undefined) canonicalRecord.company = payload.AccountName;

    return {
      tenant_id,
      record_id,
      event_id,
      source: 'system_b',
      source_timestamp: timestamp || new Date().toISOString(),
      operation,
      changed_fields: changed,
      record: canonicalRecord,
      schema_version: 1,
      rule_version: 1,
      sync_marker: payload.__sync_event_id || undefined,
    };
  }

  toDestination(
    recordId: string,
    canonicalRecord: Partial<CanonicalRecord>,
    operation: CRUDOperation,
    correlationId?: string,
  ): Record<string, any> {
    const destinationPayload: Record<string, any> = {
      Id: recordId,
      __sync_event_id: correlationId || null,
    };

    if (canonicalRecord.name !== undefined) destinationPayload.FullName = canonicalRecord.name;
    if (canonicalRecord.email !== undefined) destinationPayload.Email = canonicalRecord.email;
    if (canonicalRecord.phone !== undefined) destinationPayload.Phone = canonicalRecord.phone;
    if (canonicalRecord.company !== undefined) destinationPayload.AccountName = canonicalRecord.company;

    return destinationPayload;
  }

  async executeOperation(
    tenantId: string,
    recordId: string,
    operation: CRUDOperation,
    payload: Record<string, any>,
  ): Promise<DestinationDispatchResult> {
    // 1. Proactive Rate Limiting check via Redis Token Bucket (100 req/sec limit)
    const rateLimitKey = `ratelimit:crm:${tenantId}`;
    const tokenResult = await this.redisService.acquireToken(
      rateLimitKey,
      RATE_LIMIT_CONFIG.CRM_BURST_CAPACITY,
      RATE_LIMIT_CONFIG.CRM_MAX_RPS,
      1,
    );

    if (!tokenResult.allowed) {
      this.logger.warn(
        `Proactive throttle: CRM rate limit exceeded for tenant ${tenantId}. Retry after ${tokenResult.retryAfterMs}ms`,
      );
      return {
        success: false,
        statusCode: 429,
        retryAfterMs: tokenResult.retryAfterMs || RATE_LIMIT_CONFIG.RETRY_AFTER_DEFAULT_MS,
        error: 'Rate limit exceeded (proactive throttle)',
      };
    }

    // 2. Circuit Breaker status check
    if (!this.circuitBreaker.canExecute()) {
      this.logger.error(`Circuit breaker is OPEN for System B CRM. Deferring call.`);
      return {
        success: false,
        statusCode: 503,
        error: 'Circuit breaker is OPEN for System B CRM',
      };
    }

    // 3. Simulated external API call with failure injection support
    if (this.force429Error) {
      this.logger.warn(`Simulated Reactive HTTP 429 from External CRM`);
      return {
        success: false,
        statusCode: 429,
        retryAfterMs: 2000,
        error: 'HTTP 429 Too Many Requests (Reactive throttle)',
      };
    }

    if (this.force500Error) {
      this.circuitBreaker.recordFailure();
      this.logger.error(`Simulated HTTP 500 Internal Error from External CRM`);
      return {
        success: false,
        statusCode: 500,
        error: 'HTTP 500 Internal Server Error',
      };
    }

    try {
      // Persist CRM record to mock table for verification
      const sql = `INSERT INTO system_b_crm_records (id, full_name, email, phone, account_name, sync_event_id, last_modified_at)
                   VALUES ($1, $2, $3, $4, $5, $6, NOW())
                   ON CONFLICT (id) DO UPDATE SET
                   full_name = COALESCE($2, system_b_crm_records.full_name),
                   email = COALESCE($3, system_b_crm_records.email),
                   phone = COALESCE($4, system_b_crm_records.phone),
                   account_name = COALESCE($5, system_b_crm_records.account_name),
                   sync_event_id = COALESCE($6, system_b_crm_records.sync_event_id),
                   last_modified_at = NOW()`;

      await this.databaseService.query(sql, [
        recordId,
        payload.FullName || null,
        payload.Email || null,
        payload.Phone || null,
        payload.AccountName || null,
        payload.__sync_event_id || null,
      ]);

      this.circuitBreaker.recordSuccess();
      this.logger.log(`Successfully dispatched ${operation} to System B CRM for record ${recordId}`);
      return { success: true, statusCode: 200 };
    } catch (err: any) {
      this.circuitBreaker.recordFailure();
      this.logger.error(`Failed to dispatch operation to System B CRM: ${err.message}`);
      return { success: false, statusCode: 500, error: err.message };
    }
  }

  async readRecord(recordId: string): Promise<any> {
    const res = await this.databaseService.query('SELECT * FROM system_b_crm_records WHERE id = $1', [recordId]);
    return res.rows[0] || null;
  }
}
