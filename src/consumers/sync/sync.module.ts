import { Module, Controller, Get } from '@nestjs/common';
import { ApiTags, ApiOperation } from '@nestjs/swagger';
import { DatabaseService } from '../../infra/database.service';
import { RedisService } from '../../infra/redis.service';
import { KafkaService } from '../../infra/kafka.service';
import { CircuitBreakerService } from '../../infra/circuit-breaker.service';
import { SystemAAdapter } from '../../adapters/system-a.adapter';
import { SystemBAdapter } from '../../adapters/system-b.adapter';
import { OutboundValidatorService } from '../../engine/outbound-validator.service';
import { SyncConsumer } from './sync.consumer';

@ApiTags('Sync Consumer')
@Controller()
export class SyncConsumerController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Get('health')
  @ApiOperation({ summary: 'Health check for Sync Consumer' })
  health() {
    return {
      status: 'ok',
      service: 'sync-consumer',
      port: process.env.PORT || 3003,
      timestamp: new Date().toISOString(),
    };
  }

  @Get('status')
  @ApiOperation({ summary: 'Status check for Sync Consumer' })
  status() {
    return {
      service: 'sync-consumer',
      running: true,
      kafkaConnected: this.kafkaService.isKafkaConnected(),
      inputTopic: 'scheduled-job',
      dispatchTarget: 'outbound-systems',
      uptimeSec: Math.floor(process.uptime()),
    };
  }
}

@Module({
  controllers: [SyncConsumerController],
  providers: [
    DatabaseService,
    RedisService,
    KafkaService,
    CircuitBreakerService,
    SystemAAdapter,
    SystemBAdapter,
    OutboundValidatorService,
    SyncConsumer,
  ],
  exports: [SyncConsumer],
})
export class SyncConsumerModule {}
