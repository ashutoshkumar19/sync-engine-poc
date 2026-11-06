import { CanonicalEvent, CanonicalRecord } from '../common/types';

export const CANONICAL_FIELDS = ['name', 'email', 'phone', 'company'] as const;
export type CanonicalFieldName = (typeof CANONICAL_FIELDS)[number];

export class CanonicalSchemaValidator {
  static validate(event: Partial<CanonicalEvent>): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (!event.tenant_id) errors.push('tenant_id is required');
    if (!event.record_id) errors.push('record_id is required');
    if (!event.event_id) errors.push('event_id is required');
    if (!event.source || !['system_a', 'system_b'].includes(event.source)) {
      errors.push('source must be either system_a or system_b');
    }
    if (!event.operation || !['INSERT', 'UPDATE', 'DELETE'].includes(event.operation)) {
      errors.push('operation must be INSERT, UPDATE, or DELETE');
    }
    if (!event.record && event.operation !== 'DELETE') {
      errors.push('record payload is required for non-delete operations');
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }

  static sanitizeRecord(record: Record<string, any>): CanonicalRecord {
    const sanitized: CanonicalRecord = {};
    for (const field of CANONICAL_FIELDS) {
      if (record[field] !== undefined) {
        sanitized[field] = record[field];
      }
    }
    return sanitized;
  }
}
