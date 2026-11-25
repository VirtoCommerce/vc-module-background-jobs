import * as blades from "./pages";
import * as locales from "./locales";
import { defineAppModule } from "@vc-shell/framework";
import { markRaw } from "vue";
import JobProgressNotification from "./components/JobProgressNotification.vue";
import type { JobProgressPushNotification } from "./types/job-progress-notification";

export default defineAppModule({
  blades,
  locales,
  notifications: {
    // the key is the server's NotifyType: nameof(JobProgressPushNotification)
    JobProgressPushNotification: {
      template: markRaw(JobProgressNotification),
      toast: {
        mode: "progress",
        isComplete: (m) => !!(m as JobProgressPushNotification).finished,
        completedType: (m) => ((m as JobProgressPushNotification).outcome === "Failed" ? "error" : "success"),
      },
    },
  },
});
