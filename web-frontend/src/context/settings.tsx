import React from 'react';
import { Settings } from "../settings"

const emptySettings: Settings = {"specs": [], "structure_families": {}};
const SettingsContext = React.createContext(emptySettings);

export {emptySettings, SettingsContext};
