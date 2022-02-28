import yaml from "js-yaml";

const basename = process.env.PUBLIC_URL;

export const loadConfig = () => {
  let maybeConfig: string | null = sessionStorage.getItem("config")
  var config: string
  if (maybeConfig === null) {
    // Load UI configuration. This includes UI-specific information
    // that we cannot get from the API.
    // The configuration is specified as YAML.
    // "Why not JSON?" I hear you ask.
    // JSON is not a good configuration format because it does not support *comments*.
    // Therefore, we parse the YAML into JSON here and stash it in sessionStorage.
    fetch(`${basename}/config.yml`)
    .then((response) => response.text())
    .then((data) =>
      sessionStorage.setItem("config", JSON.stringify(yaml.load(data)))
    );
    config = sessionStorage.getItem("config") as string
  } else {
    config = maybeConfig as string
  }
  return JSON.parse(config);
};
