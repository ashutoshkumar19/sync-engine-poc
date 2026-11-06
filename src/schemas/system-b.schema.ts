export interface SystemBRecord {
  Id: string;
  FullName?: string;
  Email?: string;
  Phone?: string;
  AccountName?: string;
  __sync_event_id?: string | null; // Correlation marker sent to CRM to detect echo
  LastModifiedDate?: string;
}

export interface CRMWebhookEvent {
  tenant_id: string;
  record_id: string;
  event_id: string;
  event_type: 'CREATE' | 'UPDATE' | 'DELETE';
  changed_fields?: string[];
  payload: Partial<SystemBRecord>;
  timestamp: string; // ISO-8601
}

export interface EnvelopedSystemBEvent {
  meta: {
    type?: string;
    pipeline_id?: string;
    tenant_id: string;
    event_id: string;
  };
  data: {
    event_type: 'CREATE' | 'UPDATE' | 'DELETE';
    changed_fields?: string[];
    payload: Partial<SystemBRecord>;
    timestamp: string;
    record_id?: string;
  };
}

export class SystemBSchemaValidator {
  static unwrapEvent(raw: any): any {
    if (!raw) return raw;
    if (raw.meta && raw.data) {
      return {
        tenant_id: raw.meta.tenant_id,
        event_id: raw.meta.event_id,
        pipeline_id: raw.meta.pipeline_id,
        record_id: raw.data.record_id || raw.data.payload?.Id || raw.data.payload?.id,
        meta: raw.meta,
        ...raw.data,
      };
    }
    if (raw.event) {
      return SystemBSchemaValidator.unwrapEvent(raw.event);
    }
    return raw;
  }

  static validateWebhook(rawEvent: any): { valid: boolean; errors: string[] } {
    const event = SystemBSchemaValidator.unwrapEvent(rawEvent);
    const errors: string[] = [];
    if (!event) {
      return { valid: false, errors: ['Webhook event is empty'] };
    }
    if (!event.tenant_id) errors.push('tenant_id is required');
    if (!event.record_id) errors.push('record_id is required');
    if (!event.event_id) errors.push('event_id is required');
    if (!event.event_type || !['CREATE', 'UPDATE', 'DELETE'].includes(event.event_type)) {
      errors.push('event_type must be CREATE, UPDATE, or DELETE');
    }
    if (event.event_type !== 'DELETE' && (!event.payload || typeof event.payload !== 'object')) {
      errors.push('payload object is required for non-delete webhooks');
    }
    return { valid: errors.length === 0, errors };
  }
}
