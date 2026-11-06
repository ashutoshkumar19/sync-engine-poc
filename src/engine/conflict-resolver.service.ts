import { Injectable, Logger } from '@nestjs/common';
import { DatabaseService } from '../infra/database.service';
import { CanonicalEvent, ConflictResolutionResult, FieldState } from '../common/types';
import { parseTimestamp } from '../common/utils';
import { SYSTEM_IDS } from '../common/constants';

@Injectable()
export class ConflictResolverService {
  private readonly logger = new Logger(ConflictResolverService.name);

  constructor(private readonly databaseService: DatabaseService) {}

  /**
   * Resolve field-level conflicts using per-field Last-Write-Wins (LWW) atomically in PostgreSQL
   */
  async resolveConflicts(pipelineId: string, event: CanonicalEvent): Promise<ConflictResolutionResult> {
    const { tenant_id, record_id, event_id, source, source_timestamp, changed_fields } = event;
    const incomingTs = parseTimestamp(source_timestamp);

    return await this.databaseService.withTransaction(async (client) => {
      // 1. Lock existing sync_metadata row with SELECT ... FOR UPDATE
      const selectSql = `
        SELECT tenant_id, pipeline_id, record_id, field_state, src_sync_event_id
        FROM sync_metadata
        WHERE tenant_id = $1 AND pipeline_id = $2 AND record_id = $3
        FOR UPDATE
      `;
      const selectRes = await client.query(selectSql, [tenant_id, pipelineId, record_id]);

      let currentFieldState: FieldState = {};
      let isFirstTime = true;

      if (selectRes.rows && selectRes.rows.length > 0) {
        isFirstTime = false;
        const row = selectRes.rows[0];
        currentFieldState = typeof row.field_state === 'string' ? JSON.parse(row.field_state) : row.field_state || {};
      }

      const acceptedFields: string[] = [];
      const staleFields: string[] = [];
      let isDuplicate = false;

      const updatedFieldState: FieldState = { ...currentFieldState };

      // 2. Evaluate each changed field independently
      for (const field of changed_fields) {
        const current = currentFieldState[field];

        if (!current) {
          // No previous metadata exists -> Accept
          acceptedFields.push(field);
          updatedFieldState[field] = { ts: incomingTs, source, event_id };
        } else if (incomingTs > current.ts) {
          // Newer source timestamp -> Accept
          acceptedFields.push(field);
          updatedFieldState[field] = { ts: incomingTs, source, event_id };
        } else if (incomingTs < current.ts) {
          // Older timestamp -> Reject as stale
          staleFields.push(field);
        } else {
          // Timestamps are equal
          if (event_id === current.event_id) {
            // Identical timestamp and event ID -> Duplicate
            isDuplicate = true;
          } else {
            // Equal timestamp but different event ID -> Deterministic source-priority (system_a > system_b)
            const isIncomingPreferred = source === SYSTEM_IDS.SYSTEM_A && current.source !== SYSTEM_IDS.SYSTEM_A;
            if (isIncomingPreferred) {
              acceptedFields.push(field);
              updatedFieldState[field] = { ts: incomingTs, source, event_id };
            } else {
              staleFields.push(field);
            }
          }
        }
      }

      const isAllStale = acceptedFields.length === 0 && !isDuplicate;

      // 3. Persist updated field_state to PostgreSQL
      if (acceptedFields.length > 0) {
        const upsertSql = `
          INSERT INTO sync_metadata (tenant_id, pipeline_id, record_id, field_state, src_sync_event_id, updated_at)
          VALUES ($1, $2, $3, $4, $5, NOW())
          ON CONFLICT (tenant_id, pipeline_id, record_id)
          DO UPDATE SET field_state = $4, updated_at = NOW()
        `;
        await client.query(upsertSql, [
          tenant_id,
          pipelineId,
          record_id,
          JSON.stringify(updatedFieldState),
          source === SYSTEM_IDS.SYSTEM_B ? event_id : null, // If inbound from CRM, set src_sync_event_id for Postgres write
        ]);
      }

      this.logger.log(
        `Conflict resolution for record ${record_id}: accepted=[${acceptedFields.join(
          ',',
        )}], stale=[${staleFields.join(',')}], duplicate=${isDuplicate}`,
      );

      return {
        acceptedFields,
        staleFields,
        isDuplicate,
        isAllStale,
        updatedFieldState,
      };
    });
  }
}
