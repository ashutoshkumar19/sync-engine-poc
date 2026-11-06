import { Injectable } from '@nestjs/common';
import { RATE_LIMIT_CONFIG } from '../common/constants';
import { CircuitBreakerService } from '../infra/circuit-breaker.service';
import { DatabaseService } from '../infra/database.service';
import { KafkaService } from '../infra/kafka.service';
import { RedisService } from '../infra/redis.service';
import { JobSchedulerService } from '../scheduler/job-scheduler.service';

export interface SystemStatus {
  databaseConnected: boolean;
  kafkaConnected: boolean;
  scheduler: ReturnType<JobSchedulerService['getStatus']>;
  circuitBreaker: ReturnType<CircuitBreakerService['getState']>;
  rateLimiter: {
    crmAvailableTokens: number;
    maxRps: number;
  };
}

@Injectable()
export class SystemStatusService {
  constructor(
    private readonly databaseService: DatabaseService,
    private readonly kafkaService: KafkaService,
    private readonly redisService: RedisService,
    private readonly circuitBreaker: CircuitBreakerService,
    private readonly jobScheduler: JobSchedulerService,
  ) {}

  /**
   * Aggregates live status from all infrastructure components.
   */
  async getStatus(): Promise<SystemStatus> {
    const schedulerStatus = this.jobScheduler.getStatus();
    const breakerStatus = this.circuitBreaker.getState();
    const rateTokenStatus = await this.redisService.acquireToken('ratelimit:crm:tenant-123', 100, 100, 0);

    return {
      databaseConnected: this.databaseService.isDbConnected(),
      kafkaConnected: this.kafkaService.isKafkaConnected(),
      scheduler: schedulerStatus,
      circuitBreaker: breakerStatus,
      rateLimiter: {
        crmAvailableTokens: rateTokenStatus.remaining,
        maxRps: RATE_LIMIT_CONFIG.CRM_MAX_RPS,
      },
    };
  }
}
