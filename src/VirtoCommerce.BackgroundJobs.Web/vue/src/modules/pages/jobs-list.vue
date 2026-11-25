<template>
  <VcBlade
    :title="$t('BACKGROUND_JOBS.MENU.TITLE')"
    :toolbar-items="bladeToolbar"
    :loading="loading"
    width="50%"
  >
    <VcDataTable
      v-model:selection="selection"
      selection-mode="single"
      data-key="name"
      :items="items"
      :is-row-selectable="(x: RegisteredJobInfo) => !!x.triggerable"
      state-key="BACKGROUND_JOBS_LIST"
      :empty-state="{ icon: 'lucide-timer', title: $t('BACKGROUND_JOBS.PAGES.LIST.EMPTY') }"
    >
      <VcColumn
        id="name"
        :title="$t('BACKGROUND_JOBS.PAGES.LIST.COLUMNS.NAME')"
        always-visible
      />
      <VcColumn
        id="payloadType"
        :title="$t('BACKGROUND_JOBS.PAGES.LIST.COLUMNS.PAYLOAD_TYPE')"
      />
      <VcColumn
        id="triggerable"
        :title="$t('BACKGROUND_JOBS.PAGES.LIST.COLUMNS.TRIGGERABLE')"
      >
        <template #body="{ data: row }">
          <div class="tw-inline-flex">
            <VcStatus :variant="row.triggerable ? 'success' : 'info'">
              {{ row.triggerable ? $t("BACKGROUND_JOBS.PAGES.LIST.YES") : $t("BACKGROUND_JOBS.PAGES.LIST.NO") }}
            </VcStatus>
          </div>
        </template>
      </VcColumn>
    </VcDataTable>
  </VcBlade>
</template>

<script setup lang="ts">
import { useApiClient, useAsync, useBlade, type IBladeToolbar } from "@vc-shell/framework";
import { VcBlade, VcColumn, VcDataTable, VcStatus } from "@vc-shell/framework/ui";
import { computed, ref } from "vue";
import { useI18n } from "vue-i18n";
import { BackgroundJobsAdminClient, type RegisteredJobInfo } from "../../api_client/virtocommerce.backgroundjobs";

defineBlade({
  url: "/background-jobs",
  name: "BackgroundJobsList",
  isWorkspace: true,
});

const { t } = useI18n({ useScope: "global" });
const { openBlade } = useBlade();
const { getApiClient } = useApiClient(BackgroundJobsAdminClient);

const items = ref<RegisteredJobInfo[]>([]);
// single selection mode hands back the row itself; an array is tolerated
const selection = ref<RegisteredJobInfo | RegisteredJobInfo[]>();
const selected = computed(() => (Array.isArray(selection.value) ? selection.value[0] : selection.value));

const { loading, action: load } = useAsync(async () => {
  const client = await getApiClient();
  items.value = await client.getRegistered();
  selection.value = undefined;
});

const bladeToolbar = ref<IBladeToolbar[]>([
  { id: "refresh", title: t("BACKGROUND_JOBS.PAGES.LIST.TOOLBAR.REFRESH"), icon: "lucide-refresh-cw", clickHandler: () => load() },
  {
    id: "run",
    title: t("BACKGROUND_JOBS.PAGES.LIST.TOOLBAR.RUN"),
    icon: "lucide-play",
    permissions: "background-jobs:execute",
    disabled: computed(() => !selected.value?.triggerable),
    clickHandler: () => openBlade({ name: "BackgroundJobRun", options: { name: selected.value!.name } }),
  },
]);

load();
</script>
