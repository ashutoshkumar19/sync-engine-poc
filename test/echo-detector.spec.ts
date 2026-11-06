import { EchoDetectorService } from '../src/engine/echo-detector.service';
import { DatabaseService } from '../src/infra/database.service';

describe('EchoDetectorService', () => {
  let echoDetector: EchoDetectorService;
  let dbService: DatabaseService;

  beforeEach(() => {
    dbService = new DatabaseService();
    echoDetector = new EchoDetectorService(dbService);
  });

  it('should return isEcho: false if no sync marker is present', async () => {
    const res = await echoDetector.checkAndResolveEcho(undefined);
    expect(res.isEcho).toBe(false);
  });

  it('should return isEcho: false if marker does not match any sync_events record', async () => {
    const res = await echoDetector.checkAndResolveEcho('unknown-marker');
    expect(res.isEcho).toBe(false);
  });

  it('should detect echo, mark sync_events as RESOLVED, and return isEcho: true', async () => {
    // 1. Stage an outbound event in sync_events
    await dbService.query(
      `INSERT INTO sync_events (event_id, tenant_id, pipeline_id, record_id, destination_id, operation, payload, status, echo_status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      ['orig-event-1', 'tenant-1', 'pipe-1', 'rec-1', 'system_b', 'UPDATE', {}, 'PENDING', null],
    );

    // 2. Incoming webhook arrives with sync marker 'orig-event-1'
    const res = await echoDetector.checkAndResolveEcho('orig-event-1');
    expect(res.isEcho).toBe(true);
    expect(res.resolvedEventId).toBe('orig-event-1');

    // 3. Verify sync_events row now has echo_status = 'RESOLVED'
    const check = await dbService.query(`SELECT echo_status FROM sync_events WHERE event_id = $1`, ['orig-event-1']);
    expect(check.rows[0].echo_status).toBe('RESOLVED');
  });
});
