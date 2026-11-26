import type { PushNotification } from "@vc-shell/framework";

/** The server's JobProgressPushNotification; no swagger response carries it, so the generator emits no type. */
export interface JobProgressPushNotification extends PushNotification {
  jobId?: string;
  title?: string;
  started?: Date | string;
  finished?: Date | string;
  processedCount?: number;
  totalCount?: number;
  attempt?: number;
  totalAttempts?: number;
  outcome?: "Succeeded" | "Failed" | "Canceled" | null;
  error?: string;
}
