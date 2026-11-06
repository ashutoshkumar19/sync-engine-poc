import { Body, Controller, Get, HttpCode, HttpStatus, Optional, Param, Post, Query } from '@nestjs/common';
import { ApiBody, ApiOperation, ApiParam, ApiQuery, ApiTags } from '@nestjs/swagger';
import { PIPELINES } from '../common/constants';
import { JobSchedulerService } from '../scheduler/job-scheduler.service';
import { EventEmitService } from '../services/event-emit.service';
import { EventQueryService } from '../services/event-query.service';
import { ScenarioService } from '../services/scenario.service';
import { SystemStatusService } from '../services/system-status.service';
import { SchedulerStartDto } from './demo.dto';
import {
  EMIT_BOTH_API_BODY,
  EMIT_SYSTEM_A_API_BODY,
  EMIT_SYSTEM_B_API_BODY,
  SCHEDULER_START_API_BODY,
} from './demo.swagger';

@ApiTags('Demo & Testing')
@Controller('demo')
export class DemoController {
  constructor(
    private readonly eventEmitService: EventEmitService,
    private readonly eventQueryService: EventQueryService,
    private readonly systemStatusService: SystemStatusService,
    private readonly scenarioService: ScenarioService,
    @Optional() private readonly jobScheduler?: JobSchedulerService,
  ) {}

  // ---------------------------------------------------------------------------
  // Emit endpoints
  // ---------------------------------------------------------------------------

  @Post('emit/system-a')
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: 'Emit System A CDC Event',
    description: 'Emits a PostgreSQL WAL Debezium CDC event into the raw CDC topic',
  })
  @ApiBody(EMIT_SYSTEM_A_API_BODY)
  async emitSystemA(@Body() body: any) {
    return this.eventEmitService.emitSystemA(body);
  }

  @Post('emit/system-b')
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: 'Emit System B CRM Webhook Event',
    description: 'Emits an External CRM webhook update event into the raw webhook topic',
  })
  @ApiBody(EMIT_SYSTEM_B_API_BODY)
  async emitSystemB(@Body() body: any) {
    return this.eventEmitService.emitSystemB(body);
  }

  @Post('emit/both')
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: 'Emit Both System A & B Events Concurrently',
    description: 'Emits concurrent events to test per-field LWW resolution and bidirectional sync',
  })
  @ApiBody(EMIT_BOTH_API_BODY)
  async emitBoth(@Body() body: any) {
    return this.eventEmitService.emitBoth(body);
  }

  @Post('emit-burst')
  @ApiOperation({
    summary: 'Emit Burst of Events',
    description:
      'Emits a configurable number of System A and/or System B events in parallel. Each event has a unique ID and realistic payload.',
  })
  @ApiQuery({ name: 'count', required: true, description: 'Number of events to emit', example: 10 })
  @ApiQuery({ name: 'system_a', required: false, type: Boolean, description: 'Enable System A events', example: true })
  @ApiQuery({ name: 'system_b', required: false, type: Boolean, description: 'Enable System B events', example: true })
  @ApiQuery({ name: 'tenant_id', required: false, type: String, description: 'Tenant ID', example: 'tenant-123' })
  @ApiQuery({ name: 'pipeline_id', required: false, type: String, description: 'Pipeline ID', example: 'pipeline-1' })
  async emitBurst(
    @Query('count') count: number,
    @Query('system_a') systemA: boolean = true,
    @Query('system_b') systemB: boolean = true,
    @Query('tenant_id') tenantId: string = 'tenant-123',
    @Query('pipeline_id') pipelineId: string = 'pipeline-1',
  ) {
    return this.eventEmitService.emitBurst({ count, systemA, systemB, tenantId, pipelineId });
  }

  // ---------------------------------------------------------------------------
  // Scheduler endpoints
  // ---------------------------------------------------------------------------

  @Post('scheduler/start')
  @ApiOperation({
    summary: 'Start Background Job Scheduler',
    description: 'Starts the periodic batch dispatch interval for pending sync events',
  })
  @ApiBody(SCHEDULER_START_API_BODY)
  startScheduler(@Body() body?: SchedulerStartDto | any) {
    return this.jobScheduler?.startInterval(body?.intervalMs);
  }

  @Post('scheduler/stop')
  @ApiOperation({
    summary: 'Stop Background Job Scheduler',
    description: 'Stops the running periodic batch dispatch interval',
  })
  stopScheduler() {
    return this.jobScheduler?.stopInterval();
  }

  @Post('scheduler/trigger')
  @ApiOperation({
    summary: 'Trigger 1 Cycle of Job Scheduler',
    description: 'Manually triggers immediate execution of pending sync events without waiting for the timer',
  })
  @ApiQuery({
    name: 'pipeline_id',
    required: false,
    description: 'Optional pipeline ID (omit to trigger both pipelines)',
    example: PIPELINES.PIPELINES_1,
  })
  async triggerScheduler(@Query('pipeline_id') pipelineId?: string) {
    return this.jobScheduler?.triggerNow(pipelineId);
  }

  // ---------------------------------------------------------------------------
  // Query / inspection endpoints
  // ---------------------------------------------------------------------------

  @Get('sync-events')
  @ApiOperation({ summary: 'Inspect sync_events table' })
  @ApiQuery({ name: 'pipeline_id', required: false })
  @ApiQuery({ name: 'status', required: false })
  @ApiQuery({ name: 'record_id', required: false })
  async getSyncEvents(
    @Query('pipeline_id') pipelineId?: string,
    @Query('status') status?: string,
    @Query('record_id') recordId?: string,
  ) {
    return this.eventQueryService.getSyncEvents(pipelineId, status, recordId);
  }

  @Get('sync-metadata/:record_id')
  @ApiOperation({ summary: 'Inspect sync_metadata for a record' })
  @ApiParam({ name: 'record_id' })
  @ApiQuery({ name: 'tenant_id', required: false })
  @ApiQuery({ name: 'pipeline_id', required: false })
  async getSyncMetadata(
    @Param('record_id') recordId: string,
    @Query('tenant_id') tenantId: string = 'tenant-123',
    @Query('pipeline_id') pipelineId: string = PIPELINES.PIPELINES_1,
  ) {
    return this.eventQueryService.getSyncMetadata(recordId, tenantId, pipelineId);
  }

  @Get('records/system-a/:id')
  @ApiOperation({ summary: 'Read a System A record' })
  @ApiParam({ name: 'id' })
  async getSystemARecord(@Param('id') id: string) {
    return this.eventQueryService.getSystemARecord(id);
  }

  @Get('records/system-b/:id')
  @ApiOperation({ summary: 'Read a System B CRM record' })
  @ApiParam({ name: 'id' })
  async getSystemBRecord(@Param('id') id: string) {
    return this.eventQueryService.getSystemBRecord(id);
  }

  @Get('status')
  @ApiOperation({ summary: 'Overall system status' })
  async getStatus() {
    return this.systemStatusService.getStatus();
  }

  // ---------------------------------------------------------------------------
  // Automated scenario orchestration
  // ---------------------------------------------------------------------------

  @Post('scenarios/:scenarioName')
  @ApiOperation({
    summary: 'Run Automated Test Scenario',
    description: 'Executes an automated end-to-end sync scenario orchestration',
  })
  @ApiParam({
    name: 'scenarioName',
    enum: [
      'independent-concurrent',
      'field-conflict-stale',
      'echo-suppression-a-to-b',
      'rate-limiting-429',
      'circuit-breaker',
    ],
    description: 'Name of the automated scenario to execute',
    example: 'independent-concurrent',
  })
  async runScenario(@Param('scenarioName') scenarioName: string) {
    return this.scenarioService.runScenario(scenarioName);
  }
}
