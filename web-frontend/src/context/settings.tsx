import React from "react";
import { Settings } from "../settings";

const emptySettings: Settings = {
  api_url: "",
  specs: [],
  structure_families: {},
};
const SettingsContext = React.createContext(emptySettings);

export { emptySettings, SettingsContext };
