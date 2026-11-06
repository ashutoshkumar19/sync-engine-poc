import { ConflictResolverService } from '../src/engine/conflict-resolver.service';
import { DatabaseService } from '../src/infra/database.service';
import { CanonicalEvent } from '../src/common/types';

describe('ConflictResolverService', () => {
  let resolver: ConflictResolverService;
  let dbService: DatabaseService;

  beforeEach(() => {
    dbService = new DatabaseService();
    resolver = new ConflictResolverService(dbService);
  });

  it('should accept all fields on initial update when no metadata exists', async () => {
    const event: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-1',
      event_id: 'ev-1',
      source: 'system_a',
      source_timestamp: '2026-11-06T00:00:00.000Z',
      operation: 'INSERT',
      changed_fields: ['name', 'email'],
      record: { name: 'Alice', email: 'alice@example.com' },
      schema_version: 1,
      rule_version: 1,
    };

    const res = await resolver.resolveConflicts('pipeline-1', event);
    expect(res.acceptedFields).toEqual(['name', 'email']);
    expect(res.staleFields).toEqual([]);
    expect(res.isDuplicate).toBe(false);
  });

  it('should reject stale incoming updates with older source timestamps', async () => {
    // 1. Initial newer update
    const eventNew: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-1',
      event_id: 'ev-new',
      source: 'system_a',
      source_timestamp: '2026-11-06T01:00:00.000Z',
      operation: 'UPDATE',
      changed_fields: ['phone'],
      record: { phone: '1111111111' },
      schema_version: 1,
      rule_version: 1,
    };
    await resolver.resolveConflicts('pipeline-1', eventNew);

    // 2. Incoming older update
    const eventOld: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-1',
      event_id: 'ev-old',
      source: 'system_b',
      source_timestamp: '2026-11-06T00:30:00.000Z', // 30 minutes earlier
      operation: 'UPDATE',
      changed_fields: ['phone'],
      record: { phone: '0000000000' },
      schema_version: 1,
      rule_version: 1,
    };

    const res = await resolver.resolveConflicts('pipeline-1', eventOld);
    expect(res.acceptedFields).toEqual([]);
    expect(res.staleFields).toEqual(['phone']);
    expect(res.isAllStale).toBe(true);
  });

  it('should allow independent concurrent updates to different fields to coexist', async () => {
    // System A updates name
    const eventA: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-concurrent',
      event_id: 'ev-A',
      source: 'system_a',
      source_timestamp: '2026-11-06T02:00:00.000Z',
      operation: 'UPDATE',
      changed_fields: ['name'],
      record: { name: 'Alice Cooper' },
      schema_version: 1,
      rule_version: 1,
    };
    const resA = await resolver.resolveConflicts('pipeline-1', eventA);
    expect(resA.acceptedFields).toEqual(['name']);

    // System B updates phone concurrently
    const eventB: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-concurrent',
      event_id: 'ev-B',
      source: 'system_b',
      source_timestamp: '2026-11-06T02:00:05.000Z',
      operation: 'UPDATE',
      changed_fields: ['phone'],
      record: { phone: '9999999999' },
      schema_version: 1,
      rule_version: 1,
    };
    const resB = await resolver.resolveConflicts('pipeline-1', eventB);
    expect(resB.acceptedFields).toEqual(['phone']);

    // Check final field_state has both latest versions
    expect(resB.updatedFieldState.name.event_id).toBe('ev-A');
    expect(resB.updatedFieldState.phone.event_id).toBe('ev-B');
  });

  it('should detect duplicate event delivery with identical timestamp and event_id', async () => {
    const event: CanonicalEvent = {
      tenant_id: 'tenant-1',
      record_id: 'rec-dup',
      event_id: 'ev-dup-1',
      source: 'system_a',
      source_timestamp: '2026-11-06T03:00:00.000Z',
      operation: 'UPDATE',
      changed_fields: ['company'],
      record: { company: 'TechCorp' },
      schema_version: 1,
      rule_version: 1,
    };

    await resolver.resolveConflicts('pipeline-1', event);
    const res = await resolver.resolveConflicts('pipeline-1', event);
    expect(res.isDuplicate).toBe(true);
  });
});
