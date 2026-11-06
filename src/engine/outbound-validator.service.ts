import { Injectable, Logger } from '@nestjs/common';
import { DatabaseService } from '../infra/database.service';
import { FieldState, SyncEventRow } from '../common/types';

export interface OutboundValidationResult {
  isValid: boolean;
  isAllSuperseded: boolean;
  filteredPayload: Record<string, any>;
  supersededFields: string[];
}

@Injectable()
export class OutboundValidatorService {
  private readonly logger = new Logger(OutboundValidatorService.name);

  constructor(private readonly databaseService: DatabaseService) {}

  /**
   * Revalidates pending outbound operation fields against the latest sync_metadata
   * to prevent stale or superseded fields from being sent to destination.
   */
  async revalidateBeforeDispatch(syncEvent: SyncEventRow): Promise<OutboundValidationResult> {
    const { tenant_id, pipeline_id, record_id, event_id, payload, operation } = syncEvent;

    // Delete operations don't have individual business field checks
    if (operation === 'DELETE') {
      return {
        isValid: true,
        isAllSuperseded: false,
        filteredPayload: payload,
        supersededFields: [],
      };
    }

    try {
      const sql = `SELECT field_state FROM sync_metadata WHERE tenant_id = $1 AND pipeline_id = $2 AND record_id = $3`;
      const res = await this.databaseService.query(sql, [tenant_id, pipeline_id, record_id]);

      if (res.rows.length === 0) {
        return {
          isValid: true,
          isAllSuperseded: false,
          filteredPayload: payload,
          supersededFields: [],
        };
      }

      const rawFieldState = res.rows[0].field_state;
      const fieldState: FieldState =
        typeof rawFieldState === 'string' ? JSON.parse(rawFieldState) : rawFieldState || {};

      const supersededFields: string[] = [];
      const filteredPayload: Record<string, any> = { ...payload };

      // Map payload keys to canonical field names
      const keyToCanonical: Record<string, string> = {
        FullName: 'name',
        first_name: 'name',
        last_name: 'name',
        Email: 'email',
        email: 'email',
        Phone: 'phone',
        phone: 'phone',
        AccountName: 'company',
        company: 'company',
      };

      let evaluatedFieldCount = 0;

      for (const [key, value] of Object.entries(payload)) {
        if (key === 'Id' || key === 'id' || key === '__sync_event_id' || key === 'src_sync_event_id') {
          continue;
        }

        evaluatedFieldCount++;
        const canonicalField = keyToCanonical[key] || key.toLowerCase();
        const latestVersion = fieldState[canonicalField];

        // If the latest accepted field has a newer event_id, this field has been superseded
        if (latestVersion && latestVersion.event_id !== event_id) {
          supersededFields.push(key);
          delete filteredPayload[key];
        }
      }

      const isAllSuperseded = evaluatedFieldCount > 0 && supersededFields.length === evaluatedFieldCount;
      const isValid = !isAllSuperseded;

      if (supersededFields.length > 0) {
        this.logger.warn(
          `Record ${record_id} outbound operation ${event_id} has superseded fields: [${supersededFields.join(
            ',',
          )}]. All superseded: ${isAllSuperseded}`,
        );
      }

      return {
        isValid,
        isAllSuperseded,
        filteredPayload,
        supersededFields,
      };
    } catch (err: any) {
      this.logger.error(`Error during outbound validation: ${err.message}`);
      return {
        isValid: true,
        isAllSuperseded: false,
        filteredPayload: payload,
        supersededFields: [],
      };
    }
  }
}
