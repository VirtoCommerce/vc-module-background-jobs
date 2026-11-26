<template>
  <NotificationTemplate
    :color="color"
    :title="notification.title ?? ''"
    icon="lucide-timer"
    :notification="notification"
    @click="open"
  >
    <VcHint
      v-if="notification.description"
      class="tw-mb-1"
    >
      {{ notification.description }}
    </VcHint>
  </NotificationTemplate>
</template>

<script setup lang="ts">
import { NotificationTemplate, useBlade, useNotificationContext } from "@vc-shell/framework";
import { VcHint } from "@vc-shell/framework/ui";
import { computed } from "vue";
import type { JobProgressPushNotification } from "../types/job-progress-notification";

const notificationRef = useNotificationContext();
const notification = computed(() => notificationRef.value as JobProgressPushNotification);
const { openBlade } = useBlade();

const outcomeColors: Record<string, string> = {
  Failed: "var(--danger-500)",
  Canceled: "var(--warning-500)",
};

const color = computed(() => {
  if (!notification.value.finished) return "var(--primary-500)";
  return outcomeColors[notification.value.outcome ?? ""] ?? "var(--success-500)";
});

// a queued job has no id yet, so there is nothing to follow
function open() {
  const { jobId, title } = notification.value;
  if (jobId) {
    openBlade({ name: "BackgroundJobProgress", options: { jobId, title } });
  }
}
</script>
