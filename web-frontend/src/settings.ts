const basename = import.meta.env.BASE_URL;

const tiledUISettingsURL =
  basename.split("/").slice(0, -2).join("/") + "/tiled-ui-settings";
// Alternate idea
// const tiledUISettingsURL = import.meta.env.TILED_UI_SETTINGS || "/tiled-ui-settings";

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

interface Settings {
  api_url: string;
  specs: Spec[];
  structure_families: any;
}

const fetchSettings = async (signal: AbortSignal): Promise<Settings> => {
  const response = await fetch(tiledUISettingsURL, { signal });
  return (await response.json()) as Settings;
};

export { fetchSettings };
export type { Settings };
