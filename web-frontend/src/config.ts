import yaml from "js-yaml";

const basename = process.env.PUBLIC_URL;

export const loadConfig = async () => {
  // Try loading config from sessionStorage.
  // If not present, download and cache.
  // This is a job for Redux once we adopt Redux.
  let maybeConfig: string | null = sessionStorage.getItem("config");
  var config: string;
  if (maybeConfig === null) {
    // Load UI configuration. This includes UI-specific information
    // that we cannot get from the API.
    // The configuration is specified as YAML.
    // "Why not JSON?" I hear you ask.
    // JSON is not a good configuration format because it does not support *comments*.
    // Therefore, we parse the YAML into JSON here and stash it in sessionStorage.
    const response = await fetch(`${basename}/config.yml`);
    const data = await response.text();
    const parsedConfig = yaml.load(data);
    sessionStorage.setItem("config", JSON.stringify(parsedConfig));
    return parsedConfig;
  } else {
    config = maybeConfig as string;
    return JSON.parse(config);
  }
};
