import { Module, Controller, Get, Logger } from '@nestjs/common';
import { ApiTags, ApiOperation } from '@nestjs/swagger';
import { DatabaseService } from '../../infra/database.service';
import { RedisService } from '../../infra/redis.service';
import { KafkaService } from '../../infra/kafka.service';
import { CircuitBreakerService } from '../../infra/circuit-breaker.service';
import { SystemAAdapter } from '../../adapters/system-a.adapter';
import { SystemBAdapter } from '../../adapters/system-b.adapter';
import { NormalizationConsumer } from './normalization.consumer';

@ApiTags('Normalization Consumer')
@Controller()
export class NormalizationConsumerController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Get('health')
  @ApiOperation({ summary: 'Health check for Normalization Consumer' })
  health() {
    return {
      status: 'ok',
      service: 'normalization-consumer',
      port: process.env.PORT || 3001,
      timestamp: new Date().toISOString(),
    };
  }

  @Get('status')
  @ApiOperation({ summary: 'Status check for Normalization Consumer' })
  status() {
    return {
      service: 'normalization-consumer',
      running: true,
      kafkaConnected: this.kafkaService.isKafkaConnected(),
      topics: ['cdc-system-a-raw', 'webhook-system-b-raw'],
      targetTopic: 'sync-events-canonical',
      uptimeSec: Math.floor(process.uptime()),
    };
  }
}

@Module({
  controllers: [NormalizationConsumerController],
  providers: [
    DatabaseService,
    RedisService,
    KafkaService,
    CircuitBreakerService,
    SystemAAdapter,
    SystemBAdapter,
    NormalizationConsumer,
  ],
  exports: [NormalizationConsumer],
})
export class NormalizationConsumerModule {}
