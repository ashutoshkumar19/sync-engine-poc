import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { KafkaService } from '../infra/kafka.service';
import { KAFKA_TOPICS, PIPELINES, SCHEDULER_CONFIG } from '../common/constants';
import { ScheduledJobPayload } from '../common/types';

@Injectable()
export class JobSchedulerService implements OnModuleDestroy {
  private readonly logger = new Logger(JobSchedulerService.name);
  private timer: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;
  private intervalMs: number = SCHEDULER_CONFIG.INTERVAL_MS;
  private lastRunTime: string | null = null;

  // Registered pipelines to schedule
  private pipelines: string[] = [PIPELINES.PIPELINES_1];

  constructor(private readonly kafkaService: KafkaService) {}

  onModuleDestroy() {
    this.stopInterval();
  }

  startInterval(customIntervalMs?: number): { success: boolean; intervalMs: number; message: string } {
    if (this.isRunning) {
      return { success: false, intervalMs: this.intervalMs, message: 'Scheduler interval is already running' };
    }

    this.intervalMs = customIntervalMs || SCHEDULER_CONFIG.INTERVAL_MS;
    this.isRunning = true;

    this.timer = setInterval(async () => {
      await this.publishScheduledJobs();
    }, this.intervalMs);

    this.logger.log(`Started Job Scheduler with interval (${this.intervalMs}ms)`);
    return {
      success: true,
      intervalMs: this.intervalMs,
      message: `Started scheduler interval with ${this.intervalMs}ms for pipelines [${this.pipelines.join(', ')}]`,
    };
  }

  stopInterval(): { success: boolean; message: string } {
    if (!this.isRunning || !this.timer) {
      return { success: false, message: 'Scheduler interval is not running' };
    }

    clearInterval(this.timer);
    this.timer = null;
    this.isRunning = false;
    this.logger.log('Stopped Job Scheduler interval');
    return { success: true, message: 'Scheduler interval stopped' };
  }

  async triggerNow(pipelineId?: string): Promise<{ success: boolean; triggeredPipelines: string[] }> {
    const targetPipelines = pipelineId ? [pipelineId] : this.pipelines;
    for (const pid of targetPipelines) {
      const payload: ScheduledJobPayload = {
        pipeline_id: pid,
        timestamp: new Date().toISOString(),
      };
      await this.kafkaService.produce(KAFKA_TOPICS.SCHEDULED_JOB, pid, payload);
      this.logger.log(`Manually triggered scheduled job for pipeline: ${pid}`);
    }

    this.lastRunTime = new Date().toISOString();
    return { success: true, triggeredPipelines: targetPipelines };
  }

  private async publishScheduledJobs(): Promise<void> {
    this.lastRunTime = new Date().toISOString();
    for (const pipelineId of this.pipelines) {
      const payload: ScheduledJobPayload = {
        pipeline_id: pipelineId,
        timestamp: this.lastRunTime,
      };
      await this.kafkaService.produce(KAFKA_TOPICS.SCHEDULED_JOB, pipelineId, payload);
    }
  }

  getStatus(): { isRunning: boolean; intervalMs: number; pipelines: string[]; lastRunTime: string | null } {
    return {
      isRunning: this.isRunning,
      intervalMs: this.intervalMs,
      pipelines: this.pipelines,
      lastRunTime: this.lastRunTime,
    };
  }
}
