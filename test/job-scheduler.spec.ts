import { JobSchedulerService } from '../src/scheduler/job-scheduler.service';
import { KafkaService } from '../src/infra/kafka.service';
import { KAFKA_TOPICS, PIPELINES } from '../src/common/constants';
import { sleep } from '../src/common/utils';

describe('JobSchedulerService', () => {
  let scheduler: JobSchedulerService;
  let kafkaService: KafkaService;

  beforeEach(() => {
    kafkaService = new KafkaService();
    scheduler = new JobSchedulerService(kafkaService);
  });

  afterEach(() => {
    scheduler.stopInterval();
  });

  it('should manually trigger jobs for registered pipelines', async () => {
    const emittedJobs: any[] = [];
    jest.spyOn(kafkaService, 'produce').mockImplementation(async (topic, key, payload) => {
      if (topic === KAFKA_TOPICS.SCHEDULED_JOB) {
        emittedJobs.push(payload);
      }
    });

    const res = await scheduler.triggerNow();
    expect(res.success).toBe(true);
    expect(emittedJobs.length).toBe(Object.values(PIPELINES).length);
    expect(emittedJobs[0].pipeline_id).toBe(PIPELINES.PIPELINES_1);
  });

  it('should start and stop 10-second interval timer', async () => {
    const emittedJobs: any[] = [];
    jest.spyOn(kafkaService, 'produce').mockImplementation(async (topic, key, payload) => {
      if (topic === KAFKA_TOPICS.SCHEDULED_JOB) {
        emittedJobs.push(payload);
      }
    });

    // Test with 50ms interval for fast unit test
    const startRes = scheduler.startInterval(50);
    expect(startRes.success).toBe(true);
    expect(scheduler.getStatus().isRunning).toBe(true);

    await sleep(120);

    const stopRes = scheduler.stopInterval();
    expect(stopRes.success).toBe(true);
    expect(scheduler.getStatus().isRunning).toBe(false);
    expect(emittedJobs.length).toBeGreaterThanOrEqual(2);
  });
});
