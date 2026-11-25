import type { PushNotification } from "@vc-shell/framework";

/**
 * The server's JobProgressPushNotification (VirtoCommerce.BackgroundJobs.Core.Notifications). It is in no swagger
 * response, so the generator emits no type for it. `outcome` is the platform JobOutcome name as a string, whatever
 * the SignalR protocol, and is null while the job is queued or running.
 */
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
