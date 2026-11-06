export const KAFKA_TOPICS = {
  RAW_CDC_SYSTEM_A: 'cdc.system-a.events',
  RAW_WEBHOOK_SYSTEM_B: 'webhook.system-b.events',
  SYNC_EVENTS: 'sync-events',
  SCHEDULED_JOB: 'scheduled-job',
  SYNC_EVENTS_DLQ: 'sync-events-dlq',
} as const;

export const PIPELINES = {
  PIPELINES_1: 'pipeline-1',
} as const;

export const SYSTEM_IDS = {
  SYSTEM_A: 'system_a',
  SYSTEM_B: 'system_b',
} as const;

export const RATE_LIMIT_CONFIG = {
  CRM_MAX_RPS: 100, // 100 requests per second as specified in doc
  CRM_BURST_CAPACITY: 100,
  RETRY_AFTER_DEFAULT_MS: 1000,
  MAX_RETRIES: 3,
  BACKOFF_BASE_MS: 500,
  CIRCUIT_BREAKER_FAILURE_THRESHOLD: 5,
  CIRCUIT_BREAKER_RESET_TIMEOUT_MS: 10000, // 10 seconds
} as const;

export const SCHEDULER_CONFIG = {
  INTERVAL_MS: 10000, // 10 seconds interval as specified in doc
  BATCH_SIZE: 50, // Bounded batch size for pipeline fairness
} as const;
