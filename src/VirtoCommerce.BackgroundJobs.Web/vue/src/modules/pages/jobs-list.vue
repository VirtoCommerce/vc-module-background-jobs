<template>
  <VcBlade
    :title="$t('BACKGROUND_JOBS.MENU.TITLE')"
    :toolbar-items="bladeToolbar"
    :loading="loading"
    width="50%"
  >
    <VcDataTable
      v-model:selection="selection"
      v-model:search-value="keyword"
      selection-mode="single"
      data-key="name"
      :items="visible"
      :unfiltered-count="items.length"
      :searchable="true"
      search-bar
      :search-views="searchViews"
      :global-filters="globalFilters"
      :is-row-selectable="(x: RegisteredJobInfo) => !!x.triggerable"
      state-key="BACKGROUND_JOBS_LIST"
      :empty-state="{ icon: 'lucide-timer', title: $t('BACKGROUND_JOBS.PAGES.LIST.EMPTY') }"
      @filter="onFilter"
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
import {
  useApiClient,
  useAsync,
  useBlade,
  type GlobalFilterConfig,
  type IBladeToolbar,
  type SearchBarField,
  type SearchBarView,
} from "@vc-shell/framework";
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

const keyword = ref("");
// true matches the default search view, so the first render is already filtered, before any filter event
const triggerableOnly = ref(true);

const visible = computed(() => {
  const text = keyword.value.trim().toLowerCase();
  return items.value.filter((x) => (!triggerableOnly.value || x.triggerable) && (!text || x.name?.toLowerCase().includes(text)));
});

// GlobalFilterConfig does not list the toggle filter, SearchBarField does; the data table accepts both (as the host's tableFilters does)
const globalFilters = computed(() => {
  const fields: SearchBarField[] = [
    { id: "triggerable", label: t("BACKGROUND_JOBS.PAGES.LIST.FILTERS.TRIGGERABLE_ONLY"), filter: { toggle: true } },
  ];

  return fields as GlobalFilterConfig[];
});
const searchViews = computed<SearchBarView[]>(() => [
  { id: "triggerable", name: t("BACKGROUND_JOBS.PAGES.LIST.FILTERS.TRIGGERABLE_ONLY"), values: { triggerable: true }, default: true },
]);

function onFilter(event: { filters: Record<string, unknown> }) {
  triggerableOnly.value = !!event.filters.triggerable;
  selection.value = undefined;
}

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
