import path from "node:path";
import { getDynamicModuleConfiguration } from "@vc-shell/mf-module";

const base = "/modules/$(VirtoCommerce.BackgroundJobs)/plugins/admin-shell/";
process.env.APP_BASE_PATH = base;

export default getDynamicModuleConfiguration({
  appId: "admin-shell",
  moduleRoot: path.resolve(__dirname, ".."),
  entry: "./src/modules/index.ts",
  remoteName: "background_jobs_admin",
  base,
});
