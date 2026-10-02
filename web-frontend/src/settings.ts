// The server rewrites index.html's <base> to the prefix it serves under,
// e.g. "/tenant/tiled/ui/".
const uiBasePath = new URL(document.baseURI).pathname.replace(/\/$/, "");
const rootPath = uiBasePath.replace(/\/ui$/, "");
const bootstrapApiUrl = `${rootPath}/api/v1`;

const tiledUISettingsURL = `${rootPath}/tiled-ui-settings`;

interface Column {
  header: string;
  field: string;
  select_metadata: string;
}

interface Spec {
  spec: string;
  columns: Column[];
  default_columns: string[];
}

interface SpecView {
  spec: string;
  url: string;
}

interface Settings {
  api_url: string;
  specs: Spec[];
  spec_views?: SpecView[];
  structure_families: any;
}

const fetchSettings = async (signal: AbortSignal): Promise<Settings> => {
  const response = await fetch(tiledUISettingsURL, { signal });
  return (await response.json()) as Settings;
};

export { fetchSettings, bootstrapApiUrl, uiBasePath };
export type { Settings };
