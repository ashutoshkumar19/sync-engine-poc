import { SystemAAdapter } from '../src/adapters/system-a.adapter';
import { DatabaseService } from '../src/infra/database.service';
import { DebeziumCDCEvent } from '../src/schemas/system-a.schema';

describe('SystemAAdapter', () => {
  let adapter: SystemAAdapter;
  let dbService: DatabaseService;

  beforeEach(() => {
    dbService = new DatabaseService();
    adapter = new SystemAAdapter(dbService);
  });

  it('should normalize Debezium create event into CanonicalEvent', () => {
    const cdcEvent: DebeziumCDCEvent = {
      tenant_id: 'tenant-1',
      event_id: 'cdc-1',
      op: 'c',
      before: null,
      after: {
        id: 'rec-100',
        first_name: 'John',
        last_name: 'Doe',
        email: 'john@example.com',
        phone: '1234567890',
        company: 'Acme',
      },
      source: { ts_ms: 1700000000000, table: 'system_a_accounts' },
    };

    const canonical = adapter.toCanonical(cdcEvent);
    expect(canonical.tenant_id).toBe('tenant-1');
    expect(canonical.record_id).toBe('rec-100');
    expect(canonical.operation).toBe('INSERT');
    expect(canonical.record.name).toBe('John Doe');
    expect(canonical.record.email).toBe('john@example.com');
    expect(canonical.changed_fields).toContain('name');
    expect(canonical.changed_fields).toContain('email');
  });

  it('should detect changed fields by comparing before and after images on update', () => {
    const cdcEvent: DebeziumCDCEvent = {
      tenant_id: 'tenant-1',
      event_id: 'cdc-2',
      op: 'u',
      before: {
        id: 'rec-100',
        first_name: 'John',
        last_name: 'Doe',
        email: 'john@example.com',
        phone: '1111111111',
      },
      after: {
        id: 'rec-100',
        first_name: 'John',
        last_name: 'Doe',
        email: 'john@example.com',
        phone: '9999999999', // Phone changed
      },
      source: { ts_ms: 1700000005000, table: 'system_a_accounts' },
    };

    const canonical = adapter.toCanonical(cdcEvent);
    expect(canonical.operation).toBe('UPDATE');
    expect(canonical.changed_fields).toEqual(['phone']);
    expect(canonical.record.phone).toBe('9999999999');
  });

  it('should map canonical record to System A destination format with correlation marker', () => {
    const dest = adapter.toDestination(
      'rec-100',
      { name: 'Alice Smith', email: 'alice@example.com', phone: '55555' },
      'UPDATE',
      'corr-event-99',
    );

    expect(dest.id).toBe('rec-100');
    expect(dest.first_name).toBe('Alice');
    expect(dest.last_name).toBe('Smith');
    expect(dest.email).toBe('alice@example.com');
    expect(dest.phone).toBe('55555');
    expect(dest.src_sync_event_id).toBe('corr-event-99');
  });

  it('should execute INSERT SQL query on System A', async () => {
    const result = await adapter.executeOperation('tenant-1', 'rec-100', 'INSERT', {
      first_name: 'Bob',
      last_name: 'Marley',
      email: 'bob@reggae.com',
      phone: '123',
      company: 'Tuff Gong',
    });

    expect(result.success).toBe(true);
    const saved = await adapter.readRecord('rec-100');
    expect(saved).toBeDefined();
    expect(saved.first_name).toBe('Bob');
  });
});
