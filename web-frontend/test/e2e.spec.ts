import { test, expect } from "@playwright/test";
test("loading", async ({ page }) => {
  await page.goto("http://localhost:5173/ui/");
  await expect(page.locator("text=TILED")).toBeVisible();
  await expect(page.locator("text=Browse")).toBeVisible();
});
