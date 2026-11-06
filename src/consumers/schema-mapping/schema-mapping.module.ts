import { Module, Controller, Get } from '@nestjs/common';
import { ApiTags, ApiOperation } from '@nestjs/swagger';
import { DatabaseService } from '../../infra/database.service';
import { RedisService } from '../../infra/redis.service';
import { KafkaService } from '../../infra/kafka.service';
import { CircuitBreakerService } from '../../infra/circuit-breaker.service';
import { SystemAAdapter } from '../../adapters/system-a.adapter';
import { SystemBAdapter } from '../../adapters/system-b.adapter';
import { EchoDetectorService } from '../../engine/echo-detector.service';
import { ConflictResolverService } from '../../engine/conflict-resolver.service';
import { SchemaMappingConsumer } from './schema-mapping.consumer';

@ApiTags('Schema Mapping Consumer')
@Controller()
export class SchemaMappingConsumerController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Get('health')
  @ApiOperation({ summary: 'Health check for Schema Mapping Consumer' })
  health() {
    return {
      status: 'ok',
      service: 'schema-mapping-consumer',
      port: process.env.PORT || 3002,
      timestamp: new Date().toISOString(),
    };
  }

  @Get('status')
  @ApiOperation({ summary: 'Status check for Schema Mapping Consumer' })
  status() {
    return {
      service: 'schema-mapping-consumer',
      running: true,
      kafkaConnected: this.kafkaService.isKafkaConnected(),
      inputTopic: 'sync-events-canonical',
      stagingTable: 'sync_events',
      uptimeSec: Math.floor(process.uptime()),
    };
  }
}

@Module({
  controllers: [SchemaMappingConsumerController],
  providers: [
    DatabaseService,
    RedisService,
    KafkaService,
    CircuitBreakerService,
    SystemAAdapter,
    SystemBAdapter,
    EchoDetectorService,
    ConflictResolverService,
    SchemaMappingConsumer,
  ],
  exports: [SchemaMappingConsumer],
})
export class SchemaMappingConsumerModule {}
