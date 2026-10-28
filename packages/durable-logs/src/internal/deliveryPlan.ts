/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { createContextKey } from '@opentelemetry/api';
import type { EmitReceipt, TargetRef } from '../types';

export const DELIVERY_PLAN_KEY = createContextKey(
  'otel-durable-logs.delivery-plan'
);

export interface DeliveryPlan {
  readonly eventId: string;
  readonly matched: string[];
  readonly unavailable: string[];
  readonly available: TargetRef[];
  readonly result: Promise<EmitReceipt>;
  claimed: boolean;
  cancelled: boolean;
  resolve(receipt: EmitReceipt): void;
  reject(error: unknown): void;
}

export function createDeliveryPlan(
  eventId: string,
  matched: string[],
  unavailable: string[],
  available: TargetRef[]
): DeliveryPlan {
  let resolve!: (receipt: EmitReceipt) => void;
  let reject!: (error: unknown) => void;
  const result = new Promise<EmitReceipt>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return {
    eventId,
    matched,
    unavailable,
    available,
    result,
    claimed: false,
    cancelled: false,
    resolve,
    reject,
  };
}
