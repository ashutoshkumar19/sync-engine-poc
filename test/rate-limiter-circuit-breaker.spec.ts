import { RedisService } from '../src/infra/redis.service';
import { CircuitBreakerService, CircuitState } from '../src/infra/circuit-breaker.service';
import { sleep } from '../src/common/utils';

describe('RateLimiter & CircuitBreaker', () => {
  let redisService: RedisService;
  let circuitBreaker: CircuitBreakerService;

  beforeEach(() => {
    redisService = new RedisService();
    circuitBreaker = new CircuitBreakerService();
  });

  describe('Redis Token Bucket Rate Limiter', () => {
    it('should allow requests within capacity and replenish tokens over time', async () => {
      const key = 'ratelimit:test:crm';
      // 1. Initial acquisition
      const first = await redisService.acquireToken(key, 5, 10, 3); // capacity: 5, rate: 10/s, request: 3
      expect(first.allowed).toBe(true);
      expect(first.remaining).toBe(2);

      // 2. Consume remaining 2 tokens
      const second = await redisService.acquireToken(key, 5, 10, 2);
      expect(second.allowed).toBe(true);
      expect(second.remaining).toBe(0);

      // 3. Next immediate request should be throttled
      const third = await redisService.acquireToken(key, 5, 10, 1);
      expect(third.allowed).toBe(false);
      expect(third.retryAfterMs).toBeGreaterThan(0);

      // 4. Wait 120ms to replenish at least 1 token (10 tokens/sec = 1 token/100ms)
      await sleep(150);
      const fourth = await redisService.acquireToken(key, 5, 10, 1);
      expect(fourth.allowed).toBe(true);
    });
  });

  describe('CircuitBreakerService', () => {
    it('should transition from CLOSED to OPEN after reaching failure threshold', () => {
      expect(circuitBreaker.getState().state).toBe(CircuitState.CLOSED);

      for (let i = 0; i < 4; i++) {
        circuitBreaker.recordFailure();
        expect(circuitBreaker.canExecute()).toBe(true);
      }

      // 5th failure trips breaker
      circuitBreaker.recordFailure();
      expect(circuitBreaker.getState().state).toBe(CircuitState.OPEN);
      expect(circuitBreaker.canExecute()).toBe(false);
    });

    it('should reset failure count upon success in CLOSED state', () => {
      circuitBreaker.recordFailure();
      circuitBreaker.recordFailure();
      expect(circuitBreaker.getState().failureCount).toBe(2);

      circuitBreaker.recordSuccess();
      expect(circuitBreaker.getState().failureCount).toBe(0);
    });
  });
});
