import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class EventMetaDto {
  @ApiPropertyOptional({ example: 'internal' })
  type?: string;

  @ApiPropertyOptional({ example: 'pipeline-1' })
  pipeline_id?: string;

  @ApiProperty({ example: 'tenant-123' })
  tenant_id: string;

  @ApiProperty({ example: 'cdc-event-001' })
  event_id: string;
}

export class EmitSystemADto {
  @ApiPropertyOptional({
    description: 'Event metadata',
    type: EventMetaDto,
  })
  meta?: EventMetaDto;

  @ApiPropertyOptional({
    description: 'System A CDC event data image',
    example: {
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
  })
  data?: any;

  @ApiPropertyOptional({ description: 'Legacy raw event payload' })
  event?: any;
}

export class EmitSystemBDto {
  @ApiPropertyOptional({
    description: 'Event metadata',
    type: EventMetaDto,
  })
  meta?: EventMetaDto;

  @ApiPropertyOptional({
    description: 'System B CRM webhook update data',
    example: {
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
  })
  data?: any;

  @ApiPropertyOptional({ description: 'Legacy raw event payload' })
  event?: any;
}

export class EmitBothDto {
  @ApiPropertyOptional({
    description: 'System A CDC event (with meta & data)',
    example: {
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
  })
  system_a_event?: any;

  @ApiPropertyOptional({
    description: 'System B Webhook event (with meta & data)',
    example: {
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
  })
  system_b_event?: any;
}

export class SchedulerStartDto {
  @ApiPropertyOptional({
    description: 'Interval in milliseconds between job runs',
    example: 10000,
  })
  intervalMs?: number;
}
