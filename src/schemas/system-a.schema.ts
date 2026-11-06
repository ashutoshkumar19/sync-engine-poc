export interface SystemARecord {
  id: string;
  first_name?: string;
  last_name?: string;
  email?: string;
  phone?: string;
  company?: string;
  updated_at?: string;
}

export interface DebeziumCDCEvent {
  tenant_id: string;
  event_id: string;
  op: 'c' | 'u' | 'd' | 'r'; // Debezium standard: create, update, delete, read
  before: Partial<SystemARecord> | null;
  after: Partial<SystemARecord> | null;
  source: {
    ts_ms: number;
    table: string;
    schema?: string;
  };
  src_sync_event_id?: string | null; // Correlation marker for echo detection
}

export interface EventMeta {
  type?: string;
  pipeline_id?: string;
  tenant_id: string;
  event_id: string;
}

export interface EnvelopedSystemAEvent {
  meta: EventMeta;
  data: Omit<DebeziumCDCEvent, 'tenant_id' | 'event_id'>;
}

export class SystemASchemaValidator {
  static unwrapEvent(raw: any): any {
    if (!raw) return raw;
    if (raw.meta && raw.data) {
      return {
        tenant_id: raw.meta.tenant_id,
        event_id: raw.meta.event_id,
        pipeline_id: raw.meta.pipeline_id,
        meta: raw.meta,
        ...raw.data,
      };
    }
    if (raw.event) {
      return SystemASchemaValidator.unwrapEvent(raw.event);
    }
    return raw;
  }

  static validateCDC(rawEvent: any): { valid: boolean; errors: string[] } {
    const event = SystemASchemaValidator.unwrapEvent(rawEvent);
    const errors: string[] = [];
    if (!event) {
      return { valid: false, errors: ['Event is empty'] };
    }
    if (!event.tenant_id) errors.push('tenant_id is required');
    if (!event.event_id) errors.push('event_id is required');
    if (!event.op || !['c', 'u', 'd', 'r'].includes(event.op)) {
      errors.push('op must be c, u, d, or r');
    }
    if (event.op === 'c' && !event.after) {
      errors.push('after image required for create operation');
    }
    if (event.op === 'd' && !event.before) {
      errors.push('before image required for delete operation');
    }
    if (event.op === 'u' && (!event.before || !event.after)) {
      errors.push('both before and after images required for update operation');
    }
    return { valid: errors.length === 0, errors };
  }
}
