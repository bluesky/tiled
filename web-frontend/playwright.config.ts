import { defineConfig } from "@playwright/test";

export default defineConfig({
    testDir:'./test',
    testIgnore: [
        '**/src/**/*App.test.tsx',
    ],
});
