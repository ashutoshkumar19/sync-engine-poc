import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { RATE_LIMIT_CONFIG } from '../common/constants';

export interface TokenBucketResult {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
}

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private client: Redis | null = null;
  private isConnected: boolean = false;

  // In-memory fallback token bucket store
  private inMemoryBuckets = new Map<string, { tokens: number; lastRefill: number }>();

  // Lua script for atomic Token Bucket rate limiting
  private readonly tokenBucketLua = `
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refillRate = tonumber(ARGV[2]) -- tokens per millisecond
    local now = tonumber(ARGV[3])
    local requested = tonumber(ARGV[4])

    local data = redis.call('HMGET', key, 'tokens', 'lastRefill')
    local tokens = tonumber(data[1])
    local lastRefill = tonumber(data[2])

    if not tokens or not lastRefill then
      tokens = capacity
      lastRefill = now
    else
      local elapsed = math.max(0, now - lastRefill)
      tokens = math.min(capacity, tokens + (elapsed * refillRate))
      lastRefill = now
    end

    if tokens >= requested then
      tokens = tokens - requested
      redis.call('HMSET', key, 'tokens', tokens, 'lastRefill', lastRefill)
      redis.call('EXPIRE', key, 60)
      return { 1, math.floor(tokens), 0 }
    else
      local needed = requested - tokens
      local waitMs = math.ceil(needed / refillRate)
      redis.call('HMSET', key, 'tokens', tokens, 'lastRefill', lastRefill)
      redis.call('EXPIRE', key, 60)
      return { 0, math.floor(tokens), waitMs }
    end
  `;

  async onModuleInit() {
    const host = process.env.REDIS_HOST || 'localhost';
    const port = parseInt(process.env.REDIS_PORT || '6379', 10);

    try {
      this.client = new Redis({
        host,
        port,
        connectTimeout: 1000,
        maxRetriesPerRequest: 1,
        lazyConnect: true,
      });

      await this.client.connect();
      this.isConnected = true;
      this.logger.log(`Connected to Redis at ${host}:${port}`);
    } catch (err: any) {
      this.isConnected = false;
      this.logger.warn(`Redis connection failed (${err.message}). Using resilient in-memory token bucket.`);
    }
  }

  async onModuleDestroy() {
    if (this.client && this.isConnected) {
      await this.client.quit();
    }
  }

  /**
   * Acquire rate limit tokens using atomic Token Bucket (hierarchical support)
   */
  async acquireToken(
    key: string,
    capacity: number = RATE_LIMIT_CONFIG.CRM_BURST_CAPACITY,
    ratePerSec: number = RATE_LIMIT_CONFIG.CRM_MAX_RPS,
    requested: number = 1,
  ): Promise<TokenBucketResult> {
    const now = Date.now();
    const refillRatePerMs = ratePerSec / 1000.0;

    if (this.isConnected && this.client) {
      try {
        const result = (await this.client.eval(
          this.tokenBucketLua,
          1,
          key,
          capacity.toString(),
          refillRatePerMs.toString(),
          now.toString(),
          requested.toString(),
        )) as [number, number, number];

        return {
          allowed: result[0] === 1,
          remaining: result[1],
          retryAfterMs: result[2],
        };
      } catch (err: any) {
        this.logger.warn(`Redis eval error: ${err.message}. Falling back to in-memory.`);
      }
    }

    // In-memory fallback
    let bucket = this.inMemoryBuckets.get(key);
    if (!bucket) {
      bucket = { tokens: capacity, lastRefill: now };
      this.inMemoryBuckets.set(key, bucket);
    } else {
      const elapsed = Math.max(0, now - bucket.lastRefill);
      bucket.tokens = Math.min(capacity, bucket.tokens + elapsed * refillRatePerMs);
      bucket.lastRefill = now;
    }

    if (bucket.tokens >= requested) {
      bucket.tokens -= requested;
      return { allowed: true, remaining: Math.floor(bucket.tokens), retryAfterMs: 0 };
    } else {
      const needed = requested - bucket.tokens;
      const waitMs = Math.ceil(needed / refillRatePerMs);
      return { allowed: false, remaining: Math.floor(bucket.tokens), retryAfterMs: waitMs };
    }
  }

  async get(key: string): Promise<string | null> {
    if (this.isConnected && this.client) {
      try {
        return await this.client.get(key);
      } catch {}
    }
    return null;
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    if (this.isConnected && this.client) {
      try {
        if (ttlSeconds) {
          await this.client.set(key, value, 'EX', ttlSeconds);
        } else {
          await this.client.set(key, value);
        }
      } catch {}
    }
  }

  resetInMemory(): void {
    this.inMemoryBuckets.clear();
  }
}
