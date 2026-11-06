import { Injectable, Logger } from '@nestjs/common';
import { RATE_LIMIT_CONFIG } from '../common/constants';

export enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN',
}

@Injectable()
export class CircuitBreakerService {
  private readonly logger = new Logger(CircuitBreakerService.name);
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private lastFailureTime: number | null = null;
  private successCountInHalfOpen: number = 0;

  private readonly failureThreshold = RATE_LIMIT_CONFIG.CIRCUIT_BREAKER_FAILURE_THRESHOLD;
  private readonly resetTimeoutMs = RATE_LIMIT_CONFIG.CIRCUIT_BREAKER_RESET_TIMEOUT_MS;

  canExecute(): boolean {
    const now = Date.now();
    if (this.state === CircuitState.OPEN) {
      if (this.lastFailureTime && now - this.lastFailureTime > this.resetTimeoutMs) {
        this.logger.log('Circuit breaker entering HALF_OPEN state');
        this.state = CircuitState.HALF_OPEN;
        this.successCountInHalfOpen = 0;
        return true;
      }
      return false;
    }
    return true;
  }

  recordSuccess(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCountInHalfOpen++;
      if (this.successCountInHalfOpen >= 2) {
        this.logger.log('Circuit breaker reset to CLOSED state');
        this.state = CircuitState.CLOSED;
        this.failureCount = 0;
        this.lastFailureTime = null;
      }
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount = 0;
    }
  }

  recordFailure(): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.state === CircuitState.HALF_OPEN || this.failureCount >= this.failureThreshold) {
      this.state = CircuitState.OPEN;
      this.logger.warn(`Circuit breaker tripped to OPEN state (failures: ${this.failureCount})`);
    }
  }

  reset(): void {
    this.state = CircuitState.CLOSED;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.successCountInHalfOpen = 0;
  }

  getState(): { state: CircuitState; failureCount: number; lastFailureTime: number | null } {
    return {
      state: this.state,
      failureCount: this.failureCount,
      lastFailureTime: this.lastFailureTime,
    };
  }
}
