import "@testing-library/jest-dom";
import { beforeAll } from "vitest";

// Provide a working Web Storage implementation when the environment does not
// already supply one with functional methods. jsdom normally does, but Node
// >= 22 ships an experimental built-in `localStorage` global that shadows
// jsdom's and exposes no methods unless `--localstorage-file` is set. Without
// this shim, tests that touch localStorage fail with
// "localStorage.clear is not a function" on newer Node versions.
function installMemoryStorage() {
  const store = new Map<string, string>();
  const storage: Storage = {
    get length() {
      return store.size;
    },
    clear: () => store.clear(),
    getItem: (key: string) => (store.has(key) ? store.get(key)! : null),
    key: (index: number) => Array.from(store.keys())[index] ?? null,
    removeItem: (key: string) => void store.delete(key),
    setItem: (key: string, value: string) => void store.set(key, String(value)),
  };
  Object.defineProperty(globalThis, "localStorage", {
    value: storage,
    configurable: true,
    writable: true,
  });
  if (typeof window !== "undefined") {
    Object.defineProperty(window, "localStorage", {
      value: storage,
      configurable: true,
      writable: true,
    });
  }
}

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };

  if (
    typeof localStorage === "undefined" ||
    typeof localStorage.clear !== "function"
  ) {
    installMemoryStorage();
  }
});
