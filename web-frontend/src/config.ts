// Load UI configuration. This includes UI-specific information that we cannot
// get from the API.  The configuration is specified as YAML.  "Why not JSON?"
// I hear you ask.  JSON is not a good configuration format because it does
// not support *comments*.  Therefore, we parse the YAML into JSON here and
// stash it in sessionStorage.
import yaml from "js-yaml";

const basename = process.env.PUBLIC_URL;

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

interface Manifest {
  manifest: string[];
}

interface Config {
  specs: Spec[];
  structure_families: any;
}

const fetchManifest = async (): Promise<string[]> => {
  // Fetch a specially-named file from the public static directory.
  const response = await fetch(`${basename}/configuration_manifest.yml`);
  const text = await response.text();
  const data = yaml.load(text) as Manifest;
  return data.manifest
}

const fetchConfig = async (path: string): Promise<Config> => {
  // FastAPI StaticFiles ensures that we cannot "escape" the directory here
  // and serve arbitrary files from the filesystem.
  const response = await fetch(`${basename}/${path}`);
  const text = await response.text();
  const data = yaml.load(text) as Config;
  return data
}

export const loadConfig = async () => {
  // Try loading config from sessionStorage.
  // If not present, obtain it and cache it.
  // This is a job for Redux once we adopt Redux.
  let cachedConfig: string | null = sessionStorage.getItem("config");
  var config: string;
  if (cachedConfig === null) {
    // Config is not cached.
    const manifest = await fetchManifest();
    const configs: Config[] = await Promise.all(manifest.map((path: string) => {
      return fetchConfig(path) }
    ));
    console.log(configs);
    const mergedConfig: Config = {"specs": [], "structure_families": {}};
    configs.map((config, index) => {
      (config.specs || []).map((spec: Spec) => {
        mergedConfig.specs.push(spec);
      })
      for (const [key, value] of Object.entries(config.structure_families|| {})) {
        console.log(`${key}: ${value}`);
        mergedConfig.structure_families[key] = value;
      }
      console.log(`Loaded config ${manifest[index]}`)
    })
    console.log(mergedConfig);
    sessionStorage.setItem("config", JSON.stringify(mergedConfig));
    return mergedConfig;
  } else {
    // Config is cached.
    config = cachedConfig as string;
    return JSON.parse(config);
  }
};
