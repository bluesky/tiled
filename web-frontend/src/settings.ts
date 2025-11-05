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

function getApiBaseUrl(): string {
  if (import.meta.env.VITE_TILED_URL) {
    return import.meta.env.VITE_TILED_URL;
  }
  return import.meta.env.DEV ? '' : window.location.origin;
}


const fetchSettings = async (signal: AbortSignal): Promise<Settings> => {
  try {
    const response = await fetch(tiledUISettingsURL, { signal });
    const settings = await response.json() as Settings;
    settings.api_url = `${getApiBaseUrl()}/api/v1`;
    return settings;
  } catch (error) {
    return {
      api_url: `${getApiBaseUrl()}/api/v1`,
      specs: [],
      structure_families: {},
    };
  }
};

export { fetchSettings, getApiBaseUrl };
export type { Settings };
