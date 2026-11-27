<template>
  <VcBlade
    :title="$t('BACKGROUND_JOBS.PAGES.RUN.TITLE', { name: options?.name })"
    :toolbar-items="bladeToolbar"
    :loading="loading"
    width="40%"
  >
    <VcContainer>
      <!-- maxlength is explicit: vc-shell's default may cap a textarea, which would cut a pasted payload -->
      <VcTextarea
        v-model="payloadText"
        maxlength="1000000"
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
import { RawEnqueueClient } from "../raw-enqueue-client";

defineBlade({
  name: "BackgroundJobRun",
  routable: false,
});

const { t } = useI18n({ useScope: "global" });
const { options, openBlade } = useBlade<{ name: string }>();
const { getApiClient } = useApiClient(RawEnqueueClient);

const payloadText = ref<string>();

// The text is parsed only to validate it; what is sent is the text itself.
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
  const jobId = await client.enqueueRaw(name, payloadText.value, { reportProgress: true, title: name });
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
