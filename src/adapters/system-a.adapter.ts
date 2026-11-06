import { Injectable, Logger } from '@nestjs/common';
import { ISystemAdapter } from './adapter.interface';
import { CanonicalEvent, CanonicalRecord, CRUDOperation, DestinationDispatchResult } from '../common/types';
import { DebeziumCDCEvent, SystemASchemaValidator } from '../schemas/system-a.schema';
import { DatabaseService } from '../infra/database.service';
import { SYSTEM_IDS } from '../common/constants';

@Injectable()
export class SystemAAdapter implements ISystemAdapter {
  private readonly logger = new Logger(SystemAAdapter.name);
  readonly systemId = SYSTEM_IDS.SYSTEM_A;

  // Real SQL queries for System A database interactions
  readonly SQL_QUERIES = {
    INSERT:
      'INSERT INTO system_a_accounts (id, first_name, last_name, email, phone, company, updated_at) VALUES ($1, $2, $3, $4, $5, $6, NOW())',
    UPDATE:
      'UPDATE system_a_accounts SET first_name = COALESCE($2, first_name), last_name = COALESCE($3, last_name), email = COALESCE($4, email), phone = COALESCE($5, phone), company = COALESCE($6, company), updated_at = NOW() WHERE id = $1',
    DELETE: 'DELETE FROM system_a_accounts WHERE id = $1',
    SELECT: 'SELECT * FROM system_a_accounts WHERE id = $1',
  } as const;

  constructor(private readonly databaseService: DatabaseService) {}

  toCanonical(sourceEvent: any): CanonicalEvent {
    const unwrapped = SystemASchemaValidator.unwrapEvent(sourceEvent);
    const validation = SystemASchemaValidator.validateCDC(unwrapped);
    if (!validation.valid) {
      throw new Error(`Invalid Debezium CDC event: ${validation.errors.join(', ')}`);
    }

    const { op, before, after, tenant_id, event_id, source, src_sync_event_id } = unwrapped;

    let operation: CRUDOperation;
    switch (op) {
      case 'c':
      case 'r':
        operation = 'INSERT';
        break;
      case 'd':
        operation = 'DELETE';
        break;
      case 'u':
      default:
        operation = 'UPDATE';
        break;
    }

    const recordData = after || before || {};
    const recordId = (recordData.id || sourceEvent['record_id']) as string;

    // Detect changed fields via Debezium before and after comparison
    const changedFields: string[] = [];
    if (operation === 'INSERT') {
      if (after?.first_name || after?.last_name) changedFields.push('name');
      if (after?.email) changedFields.push('email');
      if (after?.phone) changedFields.push('phone');
      if (after?.company) changedFields.push('company');
    } else if (operation === 'UPDATE' && before && after) {
      if (before.first_name !== after.first_name || before.last_name !== after.last_name) {
        changedFields.push('name');
      }
      if (before.email !== after.email) changedFields.push('email');
      if (before.phone !== after.phone) changedFields.push('phone');
      if (before.company !== after.company) changedFields.push('company');
    } else if (operation === 'DELETE') {
      changedFields.push('name', 'email', 'phone', 'company');
    }

    // Assemble Canonical record
    const canonicalRecord: CanonicalRecord = {};
    if (recordData.first_name || recordData.last_name) {
      canonicalRecord.name = `${recordData.first_name || ''} ${recordData.last_name || ''}`.trim();
    }
    if (recordData.email) canonicalRecord.email = recordData.email;
    if (recordData.phone) canonicalRecord.phone = recordData.phone;
    if (recordData.company) canonicalRecord.company = recordData.company;

    const sourceTimestamp = source?.ts_ms ? new Date(source.ts_ms).toISOString() : new Date().toISOString();

    return {
      tenant_id,
      record_id: recordId,
      event_id,
      source: 'system_a',
      source_timestamp: sourceTimestamp,
      operation,
      changed_fields: changedFields,
      record: canonicalRecord,
      schema_version: 1,
      rule_version: 1,
      sync_marker: src_sync_event_id || undefined,
    };
  }

  toDestination(
    recordId: string,
    canonicalRecord: Partial<CanonicalRecord>,
    operation: CRUDOperation,
    correlationId?: string,
  ): Record<string, any> {
    let firstName: string | undefined = undefined;
    let lastName: string | undefined = undefined;

    if (canonicalRecord.name !== undefined) {
      const parts = canonicalRecord.name.split(' ');
      firstName = parts[0] || '';
      lastName = parts.slice(1).join(' ') || '';
    }

    return {
      id: recordId,
      first_name: firstName,
      last_name: lastName,
      email: canonicalRecord.email,
      phone: canonicalRecord.phone,
      company: canonicalRecord.company,
      src_sync_event_id: correlationId || null,
    };
  }

  async executeOperation(
    tenantId: string,
    recordId: string,
    operation: CRUDOperation,
    payload: Record<string, any>,
  ): Promise<DestinationDispatchResult> {
    try {
      if (operation === 'INSERT') {
        await this.databaseService.query(this.SQL_QUERIES.INSERT, [
          recordId,
          payload.first_name || '',
          payload.last_name || '',
          payload.email || '',
          payload.phone || '',
          payload.company || '',
        ]);
      } else if (operation === 'UPDATE') {
        await this.databaseService.query(this.SQL_QUERIES.UPDATE, [
          recordId,
          payload.first_name,
          payload.last_name,
          payload.email,
          payload.phone,
          payload.company,
        ]);
      } else if (operation === 'DELETE') {
        await this.databaseService.query(this.SQL_QUERIES.DELETE, [recordId]);
      }

      this.logger.log(`Executed ${operation} on System A for record ${recordId}`);
      return { success: true };
    } catch (err: any) {
      this.logger.error(`Failed to execute ${operation} on System A: ${err.message}`);
      return { success: false, error: err.message };
    }
  }

  async readRecord(recordId: string): Promise<any> {
    const res = await this.databaseService.query(this.SQL_QUERIES.SELECT, [recordId]);
    return res.rows[0] || null;
  }
}
