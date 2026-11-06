import { OutboundValidatorService } from '../src/engine/outbound-validator.service';
import { DatabaseService } from '../src/infra/database.service';
import { SyncEventRow } from '../src/common/types';

describe('OutboundValidatorService', () => {
  let validator: OutboundValidatorService;
  let dbService: DatabaseService;

  beforeEach(() => {
    dbService = new DatabaseService();
    validator = new OutboundValidatorService(dbService);
  });

  it('should accept operation when fields have not been superseded', async () => {
    // Record has latest version for 'phone' matching event 'ev-1'
    await dbService.query(
      `INSERT INTO sync_metadata (tenant_id, pipeline_id, record_id, field_state)
       VALUES ($1, $2, $3, $4)`,
      ['tenant-1', 'pipe-1', 'rec-1', JSON.stringify({ phone: { ts: 1000, source: 'system_a', event_id: 'ev-1' } })],
    );

    const syncEvent: SyncEventRow = {
      event_id: 'ev-1',
      tenant_id: 'tenant-1',
      pipeline_id: 'pipe-1',
      record_id: 'rec-1',
      destination_id: 'system_b',
      operation: 'UPDATE',
      payload: { Phone: '9999999999' },
      status: 'PENDING',
    };

    const res = await validator.revalidateBeforeDispatch(syncEvent);
    expect(res.isValid).toBe(true);
    expect(res.isAllSuperseded).toBe(false);
    expect(res.filteredPayload.Phone).toBe('9999999999');
  });

  it('should filter out superseded fields when a newer event was accepted in the interim', async () => {
    // Metadata shows 'phone' was updated by newer event 'ev-newer'
    await dbService.query(
      `INSERT INTO sync_metadata (tenant_id, pipeline_id, record_id, field_state)
       VALUES ($1, $2, $3, $4)`,
      [
        'tenant-1',
        'pipe-1',
        'rec-1',
        JSON.stringify({
          phone: { ts: 2000, source: 'system_a', event_id: 'ev-newer' },
          name: { ts: 1000, source: 'system_a', event_id: 'ev-1' },
        }),
      ],
    );

    const syncEvent: SyncEventRow = {
      event_id: 'ev-1',
      tenant_id: 'tenant-1',
      pipeline_id: 'pipe-1',
      record_id: 'rec-1',
      destination_id: 'system_b',
      operation: 'UPDATE',
      payload: { FullName: 'Alice', Phone: '1111111111' },
      status: 'PENDING',
    };

    const res = await validator.revalidateBeforeDispatch(syncEvent);
    expect(res.isValid).toBe(true);
    expect(res.isAllSuperseded).toBe(false);
    expect(res.supersededFields).toContain('Phone');
    expect(res.filteredPayload.Phone).toBeUndefined();
    expect(res.filteredPayload.FullName).toBe('Alice');
  });

  it('should mark operation as isAllSuperseded when every field has been superseded', async () => {
    await dbService.query(
      `INSERT INTO sync_metadata (tenant_id, pipeline_id, record_id, field_state)
       VALUES ($1, $2, $3, $4)`,
      [
        'tenant-1',
        'pipe-1',
        'rec-1',
        JSON.stringify({
          phone: { ts: 2000, source: 'system_a', event_id: 'ev-newer' },
        }),
      ],
    );

    const syncEvent: SyncEventRow = {
      event_id: 'ev-old',
      tenant_id: 'tenant-1',
      pipeline_id: 'pipe-1',
      record_id: 'rec-1',
      destination_id: 'system_b',
      operation: 'UPDATE',
      payload: { Phone: '1111111111' },
      status: 'PENDING',
    };

    const res = await validator.revalidateBeforeDispatch(syncEvent);
    expect(res.isValid).toBe(false);
    expect(res.isAllSuperseded).toBe(true);
  });
});
