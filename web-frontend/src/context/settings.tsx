import React from "react";
import { Settings, bootstrapApiUrl } from "../settings";

const emptySettings: Settings = {
  api_url: bootstrapApiUrl,
  specs: [],
  spec_views: [],
  structure_families: {},
};
const SettingsContext = React.createContext(emptySettings);

export { emptySettings, SettingsContext };
