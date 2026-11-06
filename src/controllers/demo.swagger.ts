import { ApiBodyOptions } from '@nestjs/swagger';
import { EmitSystemADto, EmitSystemBDto, EmitBothDto, SchedulerStartDto } from './demo.dto';

export const EMIT_SYSTEM_A_API_BODY: ApiBodyOptions = {
  type: EmitSystemADto,
  description: 'System A Debezium CDC event payload',
  examples: {
    updateEvent: {
      summary: 'Update Event',
      value: {
        meta: {
          type: 'internal',
          pipeline_id: 'pipeline-1',
          tenant_id: 'tenant-123',
          event_id: 'cdc-event-001',
        },
        data: {
          op: 'u',
          before: {
            id: 'rec-101',
            first_name: 'John',
            last_name: 'Doe',
            email: 'john.doe@example.com',
            phone: '+1-555-0100',
            company: 'Acme Corp',
          },
          after: {
            id: 'rec-101',
            first_name: 'Jonathan',
            last_name: 'Doe',
            email: 'jonathan.doe@example.com',
            phone: '+1-555-0199',
            company: 'Acme Corp',
          },
          source: {
            ts_ms: 1793915600000,
            table: 'system_a_accounts',
          },
        },
      },
    },
  },
};

export const EMIT_SYSTEM_B_API_BODY: ApiBodyOptions = {
  type: EmitSystemBDto,
  description: 'System B CRM Webhook event payload',
  examples: {
    updateEvent: {
      summary: 'CRM Update Event',
      value: {
        meta: {
          type: 'external',
          pipeline_id: 'pipeline-1',
          tenant_id: 'tenant-123',
          event_id: 'cdc-event-001',
        },
        data: {
          event_type: 'UPDATE',
          changed_fields: ['Phone', 'FullName'],
          payload: {
            Id: 'rec-101',
            FullName: 'Jonathan Doe',
            Phone: '+1-555-9999',
            Email: 'jonathan.doe@example.com',
            AccountName: 'Acme Corp',
          },
          timestamp: '2026-11-06T03:30:00.000Z',
        },
      },
    },
  },
};

export const EMIT_BOTH_API_BODY: ApiBodyOptions = {
  type: EmitBothDto,
  description: 'Combined System A and System B events',
  examples: {
    concurrentFieldUpdates: {
      summary: 'Concurrent Non-Conflicting Updates (Name in Sys A, Phone in Sys B)',
      value: {
        system_a_event: {
          meta: {
            type: 'internal',
            pipeline_id: 'pipeline-1',
            tenant_id: 'tenant-123',
            event_id: 'cdc-event-101',
          },
          data: {
            op: 'u',
            before: { id: 'rec-concur-1', first_name: 'John', last_name: 'Doe', phone: '111-1111' },
            after: { id: 'rec-concur-1', first_name: 'Johnny', last_name: 'Doe', phone: '111-1111' },
            source: { ts_ms: 1793915600000, table: 'system_a_accounts' },
          },
        },
        system_b_event: {
          meta: {
            type: 'internal',
            pipeline_id: 'pipeline-1',
            tenant_id: 'tenant-123',
            event_id: 'crm-event-101',
          },
          data: {
            event_type: 'UPDATE',
            changed_fields: ['Phone'],
            payload: { Id: 'rec-concur-1', Phone: '999-9999' },
            timestamp: '2026-11-06T03:30:00.000Z',
          },
        },
      },
    },
  },
};

export const SCHEDULER_START_API_BODY: ApiBodyOptions = {
  type: SchedulerStartDto,
  required: false,
  description: 'Scheduler interval configuration (optional)',
  examples: {
    defaultInterval: {
      summary: 'Standard 10-second interval',
      value: { intervalMs: 10000 },
    },
    fastInterval: {
      summary: 'Fast 1-second interval for testing',
      value: { intervalMs: 1000 },
    },
  },
};
