import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { SyncMetadata, SyncEventRow } from '../common/types';

@Injectable()
export class DatabaseService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DatabaseService.name);
  private pool: Pool | null = null;
  private isConnected: boolean = false;

  // In-memory data store for standalone testing / offline fallback
  public memorySyncMetadata = new Map<string, SyncMetadata>();
  public memorySyncEvents = new Map<string, SyncEventRow>();
  public memorySystemAAccounts = new Map<string, any>();
  public memorySystemBCRMRecords = new Map<string, any>();

  async onModuleInit() {
    const host = process.env.POSTGRES_HOST || 'localhost';
    const port = parseInt(process.env.POSTGRES_PORT || '5432', 10);
    const database = process.env.POSTGRES_DB || 'syncdb';
    const user = process.env.POSTGRES_USER || 'postgres';
    const password = process.env.POSTGRES_PASSWORD || 'postgres';

    try {
      this.pool = new Pool({
        host,
        port,
        database,
        user,
        password,
        connectionTimeoutMillis: 1000,
        idleTimeoutMillis: 5000,
        max: 10,
      });

      const client = await this.pool.connect();
      client.release();
      this.isConnected = true;
      this.logger.log(`Connected to PostgreSQL at ${host}:${port}/${database}`);
    } catch (err: any) {
      this.isConnected = false;
      this.logger.warn(`PostgreSQL connection failed (${err.message}). Using resilient in-memory database store.`);
    }
  }

  async onModuleDestroy() {
    if (this.pool && this.isConnected) {
      await this.pool.end();
    }
  }

  isDbConnected(): boolean {
    return this.isConnected;
  }

  async query<T = any>(text: string, params: any[] = []): Promise<{ rows: T[]; rowCount: number }> {
    if (this.isConnected && this.pool) {
      try {
        const res = await this.pool.query(text, params);
        return { rows: res.rows, rowCount: res.rowCount || 0 };
      } catch (err: any) {
        this.logger.error(`Postgres query error: ${err.message}. SQL: ${text}`);
        throw err;
      }
    }

    return this.executeInMemoryQuery<T>(text, params);
  }

  async withTransaction<T>(
    callback: (client: { query: (text: string, params?: any[]) => Promise<any> }) => Promise<T>,
  ): Promise<T> {
    if (this.isConnected && this.pool) {
      const client = await this.pool.connect();
      try {
        await client.query('BEGIN');
        const result = await callback(client);
        await client.query('COMMIT');
        return result;
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      } finally {
        client.release();
      }
    }

    return await callback({
      query: async (sql: string, params: any[] = []) => this.executeInMemoryQuery(sql, params),
    });
  }

  private executeInMemoryQuery<T = any>(sql: string, params: any[] = []): { rows: T[]; rowCount: number } {
    const cleanSql = sql.replace(/\s+/g, ' ').trim();

    // 1. SELECT ... FROM sync_metadata
    if (cleanSql.includes('FROM sync_metadata')) {
      const key = `${params[0]}:${params[1]}:${params[2]}`;
      const record = this.memorySyncMetadata.get(key);
      return {
        rows: (record ? [record] : []) as unknown as T[],
        rowCount: record ? 1 : 0,
      };
    }

    // 2. INSERT INTO sync_metadata ... ON CONFLICT DO UPDATE
    if (cleanSql.startsWith('INSERT INTO sync_metadata')) {
      const tenant_id = params[0];
      const pipeline_id = params[1];
      const record_id = params[2];
      const field_state = typeof params[3] === 'string' ? JSON.parse(params[3]) : params[3];
      const src_sync_event_id = params[4] || null;

      const key = `${tenant_id}:${pipeline_id}:${record_id}`;
      const record: SyncMetadata = {
        tenant_id,
        pipeline_id,
        record_id,
        field_state,
        src_sync_event_id,
        updated_at: new Date(),
      };
      this.memorySyncMetadata.set(key, record);
      return { rows: [record as unknown as T], rowCount: 1 };
    }

    // 3. INSERT INTO sync_events
    if (cleanSql.startsWith('INSERT INTO sync_events')) {
      const event_id = params[0];
      const tenant_id = params[1];
      const pipeline_id = params[2];
      const record_id = params[3];
      const destination_id = params[4];
      const operation = params[5];
      const payload = typeof params[6] === 'string' ? JSON.parse(params[6]) : params[6];
      const status = params[7] || 'PENDING';
      const echo_status = params[8] || null;

      const id = `ev-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
      const row: SyncEventRow = {
        id,
        event_id,
        tenant_id,
        pipeline_id,
        record_id,
        destination_id,
        operation,
        payload,
        status,
        echo_status,
        attempt_count: 0,
        max_retries: 3,
        created_at: new Date(),
        updated_at: new Date(),
      };
      this.memorySyncEvents.set(id, row);
      return { rows: [row as unknown as T], rowCount: 1 };
    }

    // 4. SELECT ... FROM sync_events WHERE event_id = $1
    if (cleanSql.includes('FROM sync_events') && cleanSql.includes('event_id = $1')) {
      const eventId = params[0];
      const rows = Array.from(this.memorySyncEvents.values()).filter((r) => r.event_id === eventId);
      return { rows: rows as unknown as T[], rowCount: rows.length };
    }

    // 5. UPDATE sync_events SET echo_status = 'RESOLVED' WHERE id = $1
    if (cleanSql.includes('UPDATE sync_events') && cleanSql.includes("echo_status = 'RESOLVED'")) {
      const id = params[0];
      let updatedRow: SyncEventRow | undefined;
      for (const row of this.memorySyncEvents.values()) {
        if (row.id === id || row.event_id === id) {
          row.echo_status = 'RESOLVED';
          row.updated_at = new Date();
          updatedRow = row;
          break;
        }
      }
      return { rows: (updatedRow ? [updatedRow] : []) as unknown as T[], rowCount: updatedRow ? 1 : 0 };
    }

    // 6. UPDATE sync_events SET status = 'PROCESSING'
    if (cleanSql.includes('UPDATE sync_events') && cleanSql.includes("status = 'PROCESSING'")) {
      const ids: string[] = params[0] || [];
      const updatedRows: SyncEventRow[] = [];
      for (const id of ids) {
        const item = this.memorySyncEvents.get(id);
        if (item && item.status === 'PENDING') {
          item.status = 'PROCESSING';
          item.updated_at = new Date();
          updatedRows.push(item);
        }
      }
      return { rows: updatedRows as unknown as T[], rowCount: updatedRows.length };
    }

    // 7. General UPDATE sync_events (e.g. status = 'SUCCESS', 'SUPERSEDED', 'FAILED', or 'PENDING')
    if (cleanSql.startsWith('UPDATE sync_events')) {
      const id = params[params.length - 1];
      const item =
        this.memorySyncEvents.get(id) ||
        Array.from(this.memorySyncEvents.values()).find((r) => r.id === id || r.event_id === id);
      if (item) {
        if (cleanSql.includes("status = 'SUCCESS'")) item.status = 'SUCCESS';
        else if (cleanSql.includes("status = 'SUPERSEDED'")) item.status = 'SUPERSEDED';
        else if (cleanSql.includes("status = 'FAILED'")) item.status = 'FAILED';
        else if (cleanSql.includes("status = 'PENDING'")) item.status = 'PENDING';

        if (cleanSql.includes('attempt_count = $1')) {
          item.attempt_count = params[0];
        }
        if (cleanSql.includes('next_attempt_at = $2')) {
          item.next_attempt_at = params[1];
        }
        if (cleanSql.includes('error_message')) {
          item.error_message = params[2];
        }

        item.updated_at = new Date();
        return { rows: [item as unknown as T], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    }

    // 8. Filtered SELECT * FROM sync_events
    if (cleanSql.includes('FROM sync_events')) {
      let rows = Array.from(this.memorySyncEvents.values());

      if (cleanSql.includes('pipeline_id = $1') && cleanSql.includes("status = 'PENDING'")) {
        const pipelineId = params[0];
        const limit = params[1] || 50;
        rows = rows.filter((r) => r.pipeline_id === pipelineId && r.status === 'PENDING').slice(0, limit);
        return { rows: rows as unknown as T[], rowCount: rows.length };
      }

      if (cleanSql.includes('pipeline_id = $1') && cleanSql.includes('record_id = $2')) {
        const pipelineId = params[0];
        const recordId = params[1];
        rows = rows.filter((r) => r.pipeline_id === pipelineId && r.record_id === recordId);
        return { rows: rows as unknown as T[], rowCount: rows.length };
      }

      // General query with optional WHERE params
      if (cleanSql.includes('pipeline_id = $')) {
        const idx = cleanSql.indexOf('pipeline_id = $');
        const pNum = parseInt(cleanSql[idx + 15], 10) - 1;
        if (params[pNum]) rows = rows.filter((r) => r.pipeline_id === params[pNum]);
      }
      if (cleanSql.includes('status = $')) {
        const idx = cleanSql.indexOf('status = $');
        const pNum = parseInt(cleanSql[idx + 10], 10) - 1;
        if (params[pNum]) rows = rows.filter((r) => r.status === params[pNum]);
      }
      if (cleanSql.includes('record_id = $')) {
        const idx = cleanSql.indexOf('record_id = $');
        const pNum = parseInt(cleanSql[idx + 13], 10) - 1;
        if (params[pNum]) rows = rows.filter((r) => r.record_id === params[pNum]);
      }

      return { rows: rows as unknown as T[], rowCount: rows.length };
    }

    // 9. System A Dummy SQL
    if (cleanSql.includes('system_a_accounts')) {
      if (cleanSql.startsWith('INSERT INTO system_a_accounts')) {
        const row = {
          id: params[0],
          first_name: params[1],
          last_name: params[2],
          email: params[3],
          phone: params[4],
          company: params[5],
        };
        this.memorySystemAAccounts.set(params[0], row);
        return { rows: [row as unknown as T], rowCount: 1 };
      }
      if (cleanSql.startsWith('UPDATE system_a_accounts')) {
        const existing = this.memorySystemAAccounts.get(params[0]) || { id: params[0] };
        if (params[1] !== undefined) existing.first_name = params[1];
        if (params[2] !== undefined) existing.last_name = params[2];
        if (params[3] !== undefined) existing.email = params[3];
        if (params[4] !== undefined) existing.phone = params[4];
        if (params[5] !== undefined) existing.company = params[5];
        this.memorySystemAAccounts.set(params[0], existing);
        return { rows: [existing as unknown as T], rowCount: 1 };
      }
      if (cleanSql.startsWith('DELETE FROM system_a_accounts')) {
        this.memorySystemAAccounts.delete(params[0]);
        return { rows: [], rowCount: 1 };
      }
      if (cleanSql.startsWith('SELECT')) {
        const item = this.memorySystemAAccounts.get(params[0]);
        return { rows: (item ? [item] : []) as unknown as T[], rowCount: item ? 1 : 0 };
      }
    }

    // 10. System B CRM Dummy SQL
    if (cleanSql.includes('system_b_crm_records')) {
      if (cleanSql.startsWith('INSERT') || cleanSql.startsWith('UPSERT')) {
        const row = {
          id: params[0],
          full_name: params[1],
          email: params[2],
          phone: params[3],
          account_name: params[4],
          sync_event_id: params[5],
        };
        this.memorySystemBCRMRecords.set(params[0], row);
        return { rows: [row as unknown as T], rowCount: 1 };
      }
      if (cleanSql.startsWith('SELECT')) {
        const item = this.memorySystemBCRMRecords.get(params[0]);
        return { rows: (item ? [item] : []) as unknown as T[], rowCount: item ? 1 : 0 };
      }
    }

    return { rows: [], rowCount: 0 };
  }

  resetInMemory(): void {
    this.memorySyncMetadata.clear();
    this.memorySyncEvents.clear();
    this.memorySystemAAccounts.clear();
    this.memorySystemBCRMRecords.clear();
  }
}
