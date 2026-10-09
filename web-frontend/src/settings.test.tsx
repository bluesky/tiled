import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const originalHead = document.head.innerHTML;

beforeEach(() => {
  vi.resetModules();
});

afterEach(() => {
  document.head.innerHTML = originalHead;
});

async function loadSettings(head: string) {
  document.head.innerHTML = head;
  const { uiBasePath, bootstrapApiUrl } = await import("./settings");
  return { uiBasePath, bootstrapApiUrl };
}

describe("runtime UI base", () => {
  it.each([
    ["", "/ui"],
    ["/tenant/tiled", "/tenant/tiled/ui"],
    ["/tenant/ui/tiled", "/tenant/ui/tiled/ui"],
  ])("uses root %s for UI base %s", async (rootPath, uiBasePath) => {
    expect(await loadSettings(`<base href="${rootPath}/ui/" />`)).toEqual({
      uiBasePath,
      bootstrapApiUrl: `${rootPath}/api/v1`,
    });
  });
});
