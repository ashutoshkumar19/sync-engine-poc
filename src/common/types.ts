export type CRUDOperation = 'INSERT' | 'UPDATE' | 'DELETE';

export type SyncStatus = 'PENDING' | 'PROCESSING' | 'SUCCESS' | 'FAILED' | 'SUPERSEDED';

export type EchoStatus = 'RESOLVED' | null;

export interface CanonicalRecord {
  name?: string;
  email?: string;
  phone?: string;
  company?: string;
  [key: string]: any;
}

export interface CanonicalEvent {
  tenant_id: string;
  record_id: string;
  event_id: string;
  source: 'system_a' | 'system_b';
  source_timestamp: string; // ISO-8601
  operation: CRUDOperation;
  changed_fields: string[];
  record: CanonicalRecord;
  schema_version: number;
  rule_version: number;
  sync_marker?: string; // __sync_event_id for B or src_sync_event_id for A
}

export interface FieldVersion {
  ts: number;
  source: string;
  event_id: string;
}

export type FieldState = Record<string, FieldVersion>;

export interface SyncMetadata {
  tenant_id: string;
  pipeline_id: string;
  record_id: string;
  field_state: FieldState;
  src_sync_event_id?: string | null;
  created_at?: Date;
  updated_at?: Date;
}

export interface SyncEventRow {
  id?: string;
  event_id: string;
  tenant_id: string;
  pipeline_id: string;
  record_id: string;
  destination_id: string;
  operation: CRUDOperation;
  payload: Record<string, any>;
  status: SyncStatus;
  echo_status?: EchoStatus;
  attempt_count?: number;
  max_retries?: number;
  next_attempt_at?: Date | null;
  lease_expires_at?: Date | null;
  error_message?: string | null;
  created_at?: Date;
  updated_at?: Date;
}

export interface ScheduledJobPayload {
  pipeline_id: string;
  timestamp: string;
}

export interface ConflictResolutionResult {
  acceptedFields: string[];
  staleFields: string[];
  isDuplicate: boolean;
  isAllStale: boolean;
  updatedFieldState: FieldState;
}

export interface DestinationDispatchResult {
  success: boolean;
  statusCode?: number;
  retryAfterMs?: number;
  error?: string;
  superseded?: boolean;
}
