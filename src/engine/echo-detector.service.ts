import { Injectable, Logger } from '@nestjs/common';
import { DatabaseService } from '../infra/database.service';

export interface EchoDetectionResult {
  isEcho: boolean;
  resolvedEventId?: string;
}

@Injectable()
export class EchoDetectorService {
  private readonly logger = new Logger(EchoDetectorService.name);

  constructor(private readonly databaseService: DatabaseService) {}

  /**
   * Check if an incoming event is an echo using its synchronization correlation marker
   * (e.g., __sync_event_id from CRM or src_sync_event_id from Postgres CDC)
   */
  async checkAndResolveEcho(syncMarker?: string | null): Promise<EchoDetectionResult> {
    if (!syncMarker) {
      return { isEcho: false };
    }

    try {
      // Find matching outbound operation in sync_events
      const sql = `SELECT id, event_id, echo_status FROM sync_events WHERE event_id = $1 LIMIT 1`;
      const res = await this.databaseService.query(sql, [syncMarker]);

      if (res.rows.length === 0) {
        return { isEcho: false };
      }

      const syncEvent = res.rows[0];

      // If record exists and echo_status is not RESOLVED, mark as RESOLVED and suppress echo
      if (syncEvent.echo_status !== 'RESOLVED') {
        const updateSql = `UPDATE sync_events SET echo_status = 'RESOLVED', updated_at = NOW() WHERE id = $1`;
        await this.databaseService.query(updateSql, [syncEvent.id]);
        this.logger.log(
          `Echo detected for correlation marker ${syncMarker}. Marked sync_events id ${syncEvent.id} as RESOLVED.`,
        );
        return { isEcho: true, resolvedEventId: syncMarker };
      }

      // If already resolved, it's a repeated delivery of an echo
      return { isEcho: true, resolvedEventId: syncMarker };
    } catch (err: any) {
      this.logger.error(`Error during echo detection: ${err.message}`);
      return { isEcho: false };
    }
  }
}
