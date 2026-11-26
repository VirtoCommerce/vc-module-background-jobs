<template>
  <VcBlade
    :title="job?.title ?? options?.title ?? $t('BACKGROUND_JOBS.PAGES.PROGRESS.TITLE')"
    :toolbar-items="bladeToolbar"
    width="50%"
  >
    <VcContainer>
      <div class="job-progress tw-space-y-4">
        <VcCard :header="$t('BACKGROUND_JOBS.PAGES.PROGRESS.STATE.' + state)">
          <div class="job-progress__body">
            <div class="job-progress__message">{{ message }}</div>
            <VcProgress
              v-if="!finished"
              :value="percent"
              :variant="job?.totalCount ? 'default' : 'striped'"
            />
            <dl class="job-progress__facts">
              <template v-if="processedText">
                <dt>{{ $t("BACKGROUND_JOBS.PAGES.PROGRESS.PROCESSED") }}</dt>
                <dd>{{ processedText }}</dd>
              </template>
              <template v-if="attemptText">
                <dt>{{ $t("BACKGROUND_JOBS.PAGES.PROGRESS.ATTEMPT") }}</dt>
                <dd>{{ attemptText }}</dd>
              </template>
              <dt>{{ $t("BACKGROUND_JOBS.PAGES.PROGRESS.ELAPSED") }}</dt>
              <dd>{{ elapsed }}</dd>
            </dl>
          </div>
        </VcCard>
        <VcCard
          v-if="job?.error"
          :header="$t('BACKGROUND_JOBS.PAGES.PROGRESS.ERROR')"
        >
          <div class="job-progress__error">{{ job.error }}</div>
        </VcCard>
      </div>
    </VcContainer>
  </VcBlade>
</template>

<script setup lang="ts">
import { useApiClient, useAsync, useBlade, useNotificationStore, type IBladeToolbar } from "@vc-shell/framework";
import { VcBlade, VcCard, VcContainer, VcProgress } from "@vc-shell/framework/ui";
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { JobsClient } from "../../api_client/virtocommerce.backgroundjobs";
import type { JobProgressPushNotification } from "../types/job-progress-notification";

defineBlade({
  name: "BackgroundJobProgress",
  routable: false,
});

const { t } = useI18n({ useScope: "global" });
const { options } = useBlade<{ jobId: string; title: string }>();
const { getApiClient } = useApiClient(JobsClient);
const store = useNotificationStore();

// From the notification store, not onMessage: the store also holds an update that arrived before this page opened.
const job = computed(() =>
  (store.history.value as JobProgressPushNotification[]).find(
    (x) => x.notifyType === "JobProgressPushNotification" && !!x.jobId && x.jobId === options.value?.jobId,
  ),
);

const finished = computed(() => !!job.value?.finished);

// The queued notification carries no job id, so until the first worker update the job shows as queued.
const state = computed(() => {
  if (!job.value) return "QUEUED";
  if (!finished.value) return "RUNNING";
  return (job.value.outcome ?? "Succeeded").toUpperCase();
});

const message = computed(() => (job.value ? (job.value.description ?? "") : t("BACKGROUND_JOBS.PAGES.PROGRESS.QUEUED_MESSAGE")));

const percent = computed(() =>
  job.value?.totalCount ? Math.min(100, Math.round(((job.value.processedCount ?? 0) / job.value.totalCount) * 100)) : 0,
);

const processedText = computed(() => {
  const { processedCount, totalCount } = job.value ?? {};
  if (totalCount) return `${processedCount ?? 0} / ${totalCount}`;
  return processedCount ? String(processedCount) : undefined;
});

const attemptText = computed(() => {
  const { attempt, totalAttempts } = job.value ?? {};
  if (!attempt) return undefined;
  return totalAttempts
    ? t("BACKGROUND_JOBS.PAGES.PROGRESS.ATTEMPT_OF", { attempt, total: totalAttempts })
    : t("BACKGROUND_JOBS.PAGES.PROGRESS.ATTEMPT_N", { attempt });
});

const now = ref(Date.now());
const tick = setInterval(() => (now.value = Date.now()), 1000);
watch(finished, (done) => done && clearInterval(tick));
onBeforeUnmount(() => clearInterval(tick));

const elapsed = computed(() => {
  if (!job.value?.started) return "—";
  const end = job.value.finished ? new Date(job.value.finished).getTime() : now.value;
  const s = Math.max(0, Math.floor((end - new Date(job.value.started).getTime()) / 1000));
  return [Math.floor(s / 3600), Math.floor((s % 3600) / 60), s % 60].map((x) => String(x).padStart(2, "0")).join(":");
});

// Stays set after an accepted cancel: a second cancel of a job Hangfire already deleted answers 404.
const cancelRequested = ref(false);

const { action: cancel, loading: cancelling } = useAsync(async () => {
  const client = await getApiClient();
  await client.cancel(options.value!.jobId);
  cancelRequested.value = true;
});

const bladeToolbar = ref<IBladeToolbar[]>([
  {
    id: "cancel",
    title: t("BACKGROUND_JOBS.PAGES.PROGRESS.TOOLBAR.CANCEL"),
    icon: "lucide-circle-x",
    permissions: "background_jobs:manage",
    isVisible: computed(() => !finished.value),
    disabled: computed(() => cancelling.value || cancelRequested.value),
    clickHandler: () => cancel(),
  },
]);
</script>

<style lang="scss">
.job-progress {
  padding: 20px 22px 36px;
  &__body {
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 16px;
    font-size: 14px;
    color: var(--neutrals-700);
  }
  &__message {
    color: var(--neutrals-900);
  }
  &__facts {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 8px 24px;
    margin: 0;
    dt {
      color: var(--neutrals-500);
    }
    dd {
      margin: 0;
      font-variant-numeric: tabular-nums;
    }
  }
  &__error {
    padding: 12px 16px;
    font-family: ui-monospace, monospace;
    font-size: 12px;
    color: var(--danger-600);
    white-space: pre-wrap;
  }
}
</style>
