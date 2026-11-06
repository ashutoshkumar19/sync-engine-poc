import { CanonicalEvent, CanonicalRecord, CRUDOperation, DestinationDispatchResult } from '../common/types';

export interface ISystemAdapter {
  readonly systemId: string;

  /**
   * Translates incoming source event (CDC or Webhook) into canonical event format
   */
  toCanonical(sourceEvent: any): CanonicalEvent;

  /**
   * Translates canonical record and accepted fields into destination-specific payload
   */
  toDestination(
    recordId: string,
    canonicalRecord: Partial<CanonicalRecord>,
    operation: CRUDOperation,
    correlationId?: string,
  ): Record<string, any>;

  /**
   * Executes the CRUD operation on the target destination system
   */
  executeOperation(
    tenantId: string,
    recordId: string,
    operation: CRUDOperation,
    payload: Record<string, any>,
  ): Promise<DestinationDispatchResult>;
}
