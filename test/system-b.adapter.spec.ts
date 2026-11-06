import { SystemBAdapter } from '../src/adapters/system-b.adapter';
import { RedisService } from '../src/infra/redis.service';
import { CircuitBreakerService } from '../src/infra/circuit-breaker.service';
import { DatabaseService } from '../src/infra/database.service';
import { CRMWebhookEvent } from '../src/schemas/system-b.schema';

describe('SystemBAdapter', () => {
  let adapter: SystemBAdapter;
  let redisService: RedisService;
  let circuitBreaker: CircuitBreakerService;
  let dbService: DatabaseService;

  beforeEach(() => {
    redisService = new RedisService();
    circuitBreaker = new CircuitBreakerService();
    dbService = new DatabaseService();
    adapter = new SystemBAdapter(redisService, circuitBreaker, dbService);
  });

  it('should normalize CRM webhook event into CanonicalEvent', () => {
    const webhook: CRMWebhookEvent = {
      tenant_id: 'tenant-1',
      record_id: 'crm-200',
      event_id: 'wh-1',
      event_type: 'UPDATE',
      changed_fields: ['Phone', 'FullName'],
      payload: {
        Id: 'crm-200',
        FullName: 'Sarah Connor',
        Email: 'sarah@resistance.org',
        Phone: '8005551212',
        AccountName: 'Cyberdyne',
        __sync_event_id: 'marker-123',
      },
      timestamp: '2026-11-06T00:00:00.000Z',
    };

    const canonical = adapter.toCanonical(webhook);
    expect(canonical.tenant_id).toBe('tenant-1');
    expect(canonical.record_id).toBe('crm-200');
    expect(canonical.operation).toBe('UPDATE');
    expect(canonical.record.name).toBe('Sarah Connor');
    expect(canonical.record.phone).toBe('8005551212');
    expect(canonical.sync_marker).toBe('marker-123');
    expect(canonical.changed_fields).toContain('phone');
    expect(canonical.changed_fields).toContain('name');
  });

  it('should map canonical record to CRM destination format with __sync_event_id', () => {
    const dest = adapter.toDestination(
      'crm-200',
      { name: 'Sarah Connor', phone: '8005551212' },
      'UPDATE',
      'orig-event-456',
    );

    expect(dest.Id).toBe('crm-200');
    expect(dest.FullName).toBe('Sarah Connor');
    expect(dest.Phone).toBe('8005551212');
    expect(dest.__sync_event_id).toBe('orig-event-456');
  });

  it('should enforce rate limiting and proactive throttle when tokens exhausted', async () => {
    // Consume all 100 tokens
    await redisService.acquireToken('ratelimit:crm:tenant-heavy', 100, 100, 100);

    // Next request should be rate-limited
    const result = await adapter.executeOperation('tenant-heavy', 'crm-200', 'UPDATE', { Phone: '123' });
    expect(result.success).toBe(false);
    expect(result.statusCode).toBe(429);
    expect(result.retryAfterMs).toBeGreaterThan(0);
  });

  it('should prevent execution when circuit breaker is OPEN', async () => {
    for (let i = 0; i < 5; i++) {
      circuitBreaker.recordFailure();
    }
    expect(circuitBreaker.canExecute()).toBe(false);

    const result = await adapter.executeOperation('tenant-1', 'crm-200', 'UPDATE', { Phone: '123' });
    expect(result.success).toBe(false);
    expect(result.statusCode).toBe(503);
  });
});
