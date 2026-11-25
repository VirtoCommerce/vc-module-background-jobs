<template>
  <VcBlade
    :title="$t('BACKGROUND_JOBS.PAGES.RUN.TITLE', { name: options?.name })"
    :toolbar-items="bladeToolbar"
    :loading="loading"
    width="40%"
  >
    <VcContainer>
      <VcTextarea
        v-model="payloadText"
        :label="$t('BACKGROUND_JOBS.PAGES.RUN.PAYLOAD')"
        :placeholder="$t('BACKGROUND_JOBS.PAGES.RUN.PAYLOAD_PLACEHOLDER')"
        :hint="$t('BACKGROUND_JOBS.PAGES.RUN.PAYLOAD_HINT')"
        :error-message="payloadError"
      />
    </VcContainer>
  </VcBlade>
</template>

<script setup lang="ts">
import { useApiClient, useAsync, useBlade, type IBladeToolbar } from "@vc-shell/framework";
import { VcBlade, VcContainer, VcTextarea } from "@vc-shell/framework/ui";
import { computed, ref } from "vue";
import { useI18n } from "vue-i18n";
import { BackgroundJobsAdminClient } from "../../api_client/virtocommerce.backgroundjobs";

defineBlade({
  name: "BackgroundJobRun",
  routable: false,
});

const { t } = useI18n({ useScope: "global" });
const { options, openBlade } = useBlade<{ name: string }>();
const { getApiClient } = useApiClient(BackgroundJobsAdminClient);

const payloadText = ref<string>();

// an empty payload means "no payload": the request then carries none and the job gets its default
const hasPayload = computed(() => !!payloadText.value?.trim());

const payloadError = computed(() => {
  if (!hasPayload.value) return undefined;
  try {
    JSON.parse(payloadText.value!);
  } catch (e) {
    return t("BACKGROUND_JOBS.PAGES.RUN.PAYLOAD_INVALID", { error: (e as Error).message });
  }
  return undefined;
});

const { loading, action: run } = useAsync(async () => {
  const name = options.value!.name;
  const client = await getApiClient();
  const jobId = await client.enqueue({
    name,
    payload: hasPayload.value ? JSON.parse(payloadText.value!) : undefined,
    options: { reportProgress: true, title: name },
  });
  // the BackgroundJobProgress blade is added by the progress task; it is opened here by name
  await openBlade({ name: "BackgroundJobProgress", options: { jobId, title: name } });
});

const bladeToolbar = ref<IBladeToolbar[]>([
  {
    id: "run",
    title: t("BACKGROUND_JOBS.PAGES.RUN.TOOLBAR.RUN"),
    icon: "lucide-play",
    permissions: "background-jobs:execute",
    disabled: computed(() => !!payloadError.value || loading.value),
    clickHandler: () => run(),
  },
]);
</script>
