import { Injectable } from '@nestjs/common';
import { PIPELINES } from '../common/constants';
import { SystemAAdapter } from '../adapters/system-a.adapter';
import { SystemBAdapter } from '../adapters/system-b.adapter';
import { DatabaseService } from '../infra/database.service';

export interface SyncEventsResult {
  count: number | null;
  events: any[];
}

export interface SyncMetadataResult {
  found: boolean;
  metadata: any | null;
}

export interface RecordResult {
  found: boolean;
  record: any | null;
}

@Injectable()
export class EventQueryService {
  constructor(
    private readonly databaseService: DatabaseService,
    private readonly systemAAdapter: SystemAAdapter,
    private readonly systemBAdapter: SystemBAdapter,
  ) {}

  /**
   * Queries the sync_events staging table with optional filters.
   */
  async getSyncEvents(pipelineId?: string, status?: string, recordId?: string): Promise<SyncEventsResult> {
    let sql = 'SELECT * FROM sync_events WHERE 1=1';
    const params: any[] = [];

    if (pipelineId) {
      params.push(pipelineId);
      sql += ` AND pipeline_id = $${params.length}`;
    }
    if (status) {
      params.push(status);
      sql += ` AND status = $${params.length}`;
    }
    if (recordId) {
      params.push(recordId);
      sql += ` AND record_id = $${params.length}`;
    }

    sql += ' ORDER BY created_at DESC LIMIT 50';
    const res = await this.databaseService.query(sql, params);

    return {
      count: res.rowCount,
      events: res.rows,
    };
  }

  /**
   * Retrieves the sync_metadata row for a specific record.
   */
  async getSyncMetadata(
    recordId: string,
    tenantId: string = 'tenant-123',
    pipelineId: string = PIPELINES.PIPELINES_1,
  ): Promise<SyncMetadataResult> {
    const sql = `SELECT * FROM sync_metadata WHERE tenant_id = $1 AND pipeline_id = $2 AND record_id = $3`;
    const res = await this.databaseService.query(sql, [tenantId, pipelineId, recordId]);

    return {
      found: (res.rowCount ?? 0) > 0,
      metadata: res.rows[0] || null,
    };
  }

  /**
   * Reads a System A record from the system_a_accounts table.
   */
  async getSystemARecord(id: string): Promise<RecordResult> {
    const record = await this.systemAAdapter.readRecord(id);
    return { found: !!record, record };
  }

  /**
   * Reads a System B record from the CRM store.
   */
  async getSystemBRecord(id: string): Promise<RecordResult> {
    const record = await this.systemBAdapter.readRecord(id);
    return { found: !!record, record };
  }
}
