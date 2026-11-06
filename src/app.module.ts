import { Module } from '@nestjs/common';
import { DatabaseService } from './infra/database.service';
import { RedisService } from './infra/redis.service';
import { KafkaService } from './infra/kafka.service';
import { CircuitBreakerService } from './infra/circuit-breaker.service';
import { SystemAAdapter } from './adapters/system-a.adapter';
import { SystemBAdapter } from './adapters/system-b.adapter';
import { EchoDetectorService } from './engine/echo-detector.service';
import { ConflictResolverService } from './engine/conflict-resolver.service';
import { OutboundValidatorService } from './engine/outbound-validator.service';
import { JobSchedulerService } from './scheduler/job-scheduler.service';
import { DemoController } from './controllers/demo.controller';
import { EventEmitService } from './services/event-emit.service';
import { EventQueryService } from './services/event-query.service';
import { SystemStatusService } from './services/system-status.service';
import { ScenarioService } from './services/scenario.service';

@Module({
  imports: [],
  controllers: [DemoController],
  providers: [
    // Infrastructure
    DatabaseService,
    RedisService,
    KafkaService,
    CircuitBreakerService,

    // Adapters
    SystemAAdapter,
    SystemBAdapter,

    // Core Engine
    EchoDetectorService,
    ConflictResolverService,
    OutboundValidatorService,

    // Schedulers
    JobSchedulerService,

    // Demo / API services
    EventEmitService,
    EventQueryService,
    SystemStatusService,
    ScenarioService,
  ],
  exports: [
    DatabaseService,
    RedisService,
    KafkaService,
    CircuitBreakerService,
    SystemAAdapter,
    SystemBAdapter,
    EchoDetectorService,
    ConflictResolverService,
    OutboundValidatorService,
    JobSchedulerService,
    EventEmitService,
    EventQueryService,
    SystemStatusService,
    ScenarioService,
  ],
})
export class AppModule {}
