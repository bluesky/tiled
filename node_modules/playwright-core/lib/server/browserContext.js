"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var browserContext_exports = {};
__export(browserContext_exports, {
  BrowserContext: () => BrowserContext,
  normalizeProxySettings: () => normalizeProxySettings,
  validateBrowserContextOptions: () => validateBrowserContextOptions,
  verifyClientCertificates: () => verifyClientCertificates,
  verifyGeolocation: () => verifyGeolocation
});
module.exports = __toCommonJS(browserContext_exports);
var import_fs = __toESM(require("fs"));
var import_path = __toESM(require("path"));
var import_crypto = require("./utils/crypto");
var import_debug = require("./utils/debug");
var import_clock = require("./clock");
var import_debugger = require("./debugger");
var import_dialog = require("./dialog");
var import_fetch = require("./fetch");
var import_fileUtils = require("./utils/fileUtils");
var import_harRecorder = require("./har/harRecorder");
var import_helper = require("./helper");
var import_instrumentation = require("./instrumentation");
var network = __toESM(require("./network"));
var import_page = require("./page");
var import_page2 = require("./page");
var import_recorder = require("./recorder");
var import_recorderApp = require("./recorder/recorderApp");
var import_selectors = require("./selectors");
var import_tracing = require("./trace/recorder/tracing");
var rawStorageSource = __toESM(require("../generated/storageScriptSource"));
class BrowserContext extends import_instrumentation.SdkObject {
  constructor(browser, options, browserContextId) {
    super(browser, "browser-context");
    this._pageBindings = /* @__PURE__ */ new Map();
    this._activeProgressControllers = /* @__PURE__ */ new Set();
    this.requestInterceptors = [];
    this._closedStatus = "open";
    this._permissions = /* @__PURE__ */ new Map();
    this._downloads = /* @__PURE__ */ new Set();
    this._origins = /* @__PURE__ */ new Set();
    this._harRecorders = /* @__PURE__ */ new Map();
    this._tempDirs = [];
    this._settingStorageState = false;
    this.initScripts = [];
    this._routesInFlight = /* @__PURE__ */ new Set();
    this._playwrightBindingExposed = false;
    this.attribution.context = this;
    this._browser = browser;
    this._options = options;
    this._browserContextId = browserContextId;
    this._isPersistentContext = !browserContextId;
    this._closePromise = new Promise((fulfill) => this._closePromiseFulfill = fulfill);
    this._selectors = new import_selectors.Selectors(options.selectorEngines || [], options.testIdAttributeName);
    this.fetchRequest = new import_fetch.BrowserContextAPIRequestContext(this);
    this.tracing = new import_tracing.Tracing(this, browser.options.tracesDir);
    this.clock = new import_clock.Clock(this);
    this.dialogManager = new import_dialog.DialogManager(this.instrumentation);
  }
  static {
    this.Events = {
      Console: "console",
      Close: "close",
      Page: "page",
      // Can't use just 'error' due to node.js special treatment of error events.
      // @see https://nodejs.org/api/events.html#events_error_events
      PageError: "pageerror",
      Request: "request",
      Response: "response",
      RequestFailed: "requestfailed",
      RequestFinished: "requestfinished",
      RequestAborted: "requestaborted",
      RequestFulfilled: "requestfulfilled",
      RequestContinued: "requestcontinued",
      BeforeClose: "beforeclose",
      VideoStarted: "videostarted"
    };
  }
  isPersistentContext() {
    return this._isPersistentContext;
  }
  selectors() {
    return this._selectors;
  }
  async _initialize() {
    if (this.attribution.playwright.options.isInternalPlaywright)
      return;
    this._debugger = new import_debugger.Debugger(this);
    if ((0, import_debug.debugMode)() === "inspector")
      await import_recorder.Recorder.show(this, import_recorderApp.RecorderApp.factory(this), { pauseOnNextStatement: true });
    if (this._debugger.isPaused())
      import_recorder.Recorder.showInspectorNoReply(this, import_recorderApp.RecorderApp.factory(this));
    this._debugger.on(import_debugger.Debugger.Events.PausedStateChanged, () => {
      if (this._debugger.isPaused())
        import_recorder.Recorder.showInspectorNoReply(this, import_recorderApp.RecorderApp.factory(this));
    });
    if ((0, import_debug.debugMode)() === "console")
      await this.extendInjectedScript("function Console(injectedScript) { injectedScript.consoleApi.install(); }");
    if (this._options.serviceWorkers === "block")
      await this.addInitScript(`
if (navigator.serviceWorker) navigator.serviceWorker.register = async () => { console.warn('Service Worker registration blocked by Playwright'); };
`);
    if (this._options.permissions)
      await this.grantPermissions(this._options.permissions);
  }
  debugger() {
    return this._debugger;
  }
  async _ensureVideosPath() {
    if (this._options.recordVideo)
      await (0, import_fileUtils.mkdirIfNeeded)(import_path.default.join(this._options.recordVideo.dir, "dummy"));
  }
  canResetForReuse() {
    if (this._closedStatus !== "open")
      return false;
    return true;
  }
  async stopPendingOperations(reason) {
    for (const controller of this._activeProgressControllers)
      controller.abort(new Error(reason));
    await new Promise((f) => setTimeout(f, 0));
  }
  static reusableContextHash(params) {
    const paramsCopy = { ...params };
    if (paramsCopy.selectorEngines?.length === 0)
      delete paramsCopy.selectorEngines;
    for (const k of Object.keys(paramsCopy)) {
      const key = k;
      if (paramsCopy[key] === defaultNewContextParamValues[key])
        delete paramsCopy[key];
    }
    for (const key of paramsThatAllowContextReuse)
      delete paramsCopy[key];
    return JSON.stringify(paramsCopy);
  }
  async resetForReuse(metadata, params) {
    await this.tracing.resetForReuse();
    if (params) {
      for (const key of paramsThatAllowContextReuse)
        this._options[key] = params[key];
      if (params.testIdAttributeName)
        this.selectors().setTestIdAttributeName(params.testIdAttributeName);
    }
    let page = this.pages()[0];
    const [, ...otherPages] = this.pages();
    for (const p of otherPages)
      await p.close(metadata);
    if (page && page.hasCrashed()) {
      await page.close(metadata);
      page = void 0;
    }
    await page?.mainFrame().goto(metadata, "about:blank", { timeout: 0 });
    await this._resetStorage();
    await this.clock.resetForReuse();
    if (this._options.permissions)
      await this.grantPermissions(this._options.permissions);
    else
      await this.clearPermissions();
    await this.setExtraHTTPHeaders(this._options.extraHTTPHeaders || []);
    await this.setGeolocation(this._options.geolocation);
    await this.setOffline(!!this._options.offline);
    await this.setUserAgent(this._options.userAgent);
    await this.clearCache();
    await this._resetCookies();
    await page?.resetForReuse(metadata);
  }
  _browserClosed() {
    for (const page of this.pages())
      page._didClose();
    this._didCloseInternal();
  }
  _didCloseInternal() {
    if (this._closedStatus === "closed") {
      return;
    }
    this._clientCertificatesProxy?.close().catch(() => {
    });
    this.tracing.abort();
    if (this._isPersistentContext)
      this.onClosePersistent();
    this._closePromiseFulfill(new Error("Context closed"));
    this.emit(BrowserContext.Events.Close);
  }
  pages() {
    return this.possiblyUninitializedPages().filter((page) => page.initializedOrUndefined());
  }
  async cookies(urls = []) {
    if (urls && !Array.isArray(urls))
      urls = [urls];
    return await this.doGetCookies(urls);
  }
  async clearCookies(options) {
    const currentCookies = await this.cookies();
    await this.doClearCookies();
    const matches = (cookie, prop, value) => {
      if (!value)
        return true;
      if (value instanceof RegExp) {
        value.lastIndex = 0;
        return value.test(cookie[prop]);
      }
      return cookie[prop] === value;
    };
    const cookiesToReadd = currentCookies.filter((cookie) => {
      return !matches(cookie, "name", options.name) || !matches(cookie, "domain", options.domain) || !matches(cookie, "path", options.path);
    });
    await this.addCookies(cookiesToReadd);
  }
  setHTTPCredentials(httpCredentials) {
    return this.doSetHTTPCredentials(httpCredentials);
  }
  hasBinding(name) {
    return this._pageBindings.has(name);
  }
  async exposePlaywrightBindingIfNeeded() {
    if (this._playwrightBindingExposed)
      return;
    this._playwrightBindingExposed = true;
    await this.doExposePlaywrightBinding();
    this.bindingsInitScript = import_page2.PageBinding.createInitScript();
    this.initScripts.push(this.bindingsInitScript);
    await this.doAddInitScript(this.bindingsInitScript);
    await this.safeNonStallingEvaluateInAllFrames(this.bindingsInitScript.source, "main");
  }
  needsPlaywrightBinding() {
    return this._playwrightBindingExposed;
  }
  async exposeBinding(name, needsHandle, playwrightBinding) {
    if (this._pageBindings.has(name))
      throw new Error(`Function "${name}" has been already registered`);
    for (const page of this.pages()) {
      if (page.getBinding(name))
        throw new Error(`Function "${name}" has been already registered in one of the pages`);
    }
    await this.exposePlaywrightBindingIfNeeded();
    const binding = new import_page2.PageBinding(name, playwrightBinding, needsHandle);
    this._pageBindings.set(name, binding);
    await this.doAddInitScript(binding.initScript);
    await this.safeNonStallingEvaluateInAllFrames(binding.initScript.source, "main");
    return binding;
  }
  async removeExposedBindings(bindings) {
    bindings = bindings.filter((binding) => this._pageBindings.get(binding.name) === binding);
    for (const binding of bindings)
      this._pageBindings.delete(binding.name);
    await this.doRemoveInitScripts(bindings.map((binding) => binding.initScript));
    const cleanup = bindings.map((binding) => `{ ${binding.cleanupScript} };
`).join("");
    await this.safeNonStallingEvaluateInAllFrames(cleanup, "main");
  }
  async grantPermissions(permissions, origin) {
    let resolvedOrigin = "*";
    if (origin) {
      const url = new URL(origin);
      resolvedOrigin = url.origin;
    }
    const existing = new Set(this._permissions.get(resolvedOrigin) || []);
    permissions.forEach((p) => existing.add(p));
    const list = [...existing.values()];
    this._permissions.set(resolvedOrigin, list);
    await this.doGrantPermissions(resolvedOrigin, list);
  }
  async clearPermissions() {
    this._permissions.clear();
    await this.doClearPermissions();
  }
  async _loadDefaultContextAsIs(progress) {
    if (!this.possiblyUninitializedPages().length) {
      const waitForEvent = import_helper.helper.waitForEvent(progress, this, BrowserContext.Events.Page);
      progress.cleanupWhenAborted(() => waitForEvent.dispose);
      await Promise.race([waitForEvent.promise, this._closePromise]);
    }
    const page = this.possiblyUninitializedPages()[0];
    if (!page)
      return;
    const pageOrError = await page.waitForInitializedOrError();
    if (pageOrError instanceof Error)
      throw pageOrError;
    await page.mainFrame()._waitForLoadState(progress, "load");
    return page;
  }
  async _loadDefaultContext(progress) {
    const defaultPage = await this._loadDefaultContextAsIs(progress);
    if (!defaultPage)
      return;
    const browserName = this._browser.options.name;
    if (this._options.isMobile && browserName === "chromium" || this._options.locale && browserName === "webkit") {
      await this.newPage(progress.metadata);
      await defaultPage.close(progress.metadata);
    }
  }
  _authenticateProxyViaHeader() {
    const proxy = this._options.proxy || this._browser.options.proxy || { username: void 0, password: void 0 };
    const { username, password } = proxy;
    if (username) {
      this._options.httpCredentials = { username, password };
      const token = Buffer.from(`${username}:${password}`).toString("base64");
      this._options.extraHTTPHeaders = network.mergeHeaders([
        this._options.extraHTTPHeaders,
        network.singleHeader("Proxy-Authorization", `Basic ${token}`)
      ]);
    }
  }
  _authenticateProxyViaCredentials() {
    const proxy = this._options.proxy || this._browser.options.proxy;
    if (!proxy)
      return;
    const { username, password } = proxy;
    if (username)
      this._options.httpCredentials = { username, password: password || "" };
  }
  async addInitScript(source, name) {
    const initScript = new import_page.InitScript(source, name);
    this.initScripts.push(initScript);
    await this.doAddInitScript(initScript);
    return initScript;
  }
  async removeInitScripts(initScripts) {
    const set = new Set(initScripts);
    this.initScripts = this.initScripts.filter((script) => !set.has(script));
    await this.doRemoveInitScripts(initScripts);
  }
  async addRequestInterceptor(handler) {
    this.requestInterceptors.push(handler);
    await this.doUpdateRequestInterception();
  }
  async removeRequestInterceptor(handler) {
    const index = this.requestInterceptors.indexOf(handler);
    if (index === -1)
      return;
    this.requestInterceptors.splice(index, 1);
    await this.notifyRoutesInFlightAboutRemovedHandler(handler);
    await this.doUpdateRequestInterception();
  }
  isClosingOrClosed() {
    return this._closedStatus !== "open";
  }
  async _deleteAllDownloads() {
    await Promise.all(Array.from(this._downloads).map((download) => download.artifact.deleteOnContextClose()));
  }
  async _deleteAllTempDirs() {
    await Promise.all(this._tempDirs.map(async (dir) => await import_fs.default.promises.unlink(dir).catch((e) => {
    })));
  }
  setCustomCloseHandler(handler) {
    this._customCloseHandler = handler;
  }
  async close(options) {
    if (this._closedStatus === "open") {
      if (options.reason)
        this._closeReason = options.reason;
      this.emit(BrowserContext.Events.BeforeClose);
      this._closedStatus = "closing";
      for (const harRecorder of this._harRecorders.values())
        await harRecorder.flush();
      await this.tracing.flush();
      const promises = [];
      for (const { context, artifact } of this._browser._idToVideo.values()) {
        if (context === this)
          promises.push(artifact.finishedPromise());
      }
      if (this._customCloseHandler) {
        await this._customCloseHandler();
      } else {
        await this.doClose(options.reason);
      }
      promises.push(this._deleteAllDownloads());
      promises.push(this._deleteAllTempDirs());
      await Promise.all(promises);
      if (!this._customCloseHandler)
        this._didCloseInternal();
    }
    await this._closePromise;
  }
  async newPage(metadata) {
    const page = await this.doCreateNewPage(metadata.isServerSide);
    const pageOrError = await page.waitForInitializedOrError();
    if (pageOrError instanceof import_page2.Page) {
      if (pageOrError.isClosed())
        throw new Error("Page has been closed.");
      return pageOrError;
    }
    throw pageOrError;
  }
  addVisitedOrigin(origin) {
    this._origins.add(origin);
  }
  async storageState(indexedDB = false) {
    const result = {
      cookies: await this.cookies(),
      origins: []
    };
    const originsToSave = new Set(this._origins);
    const collectScript = `(() => {
      const module = {};
      ${rawStorageSource.source}
      const script = new (module.exports.StorageScript())(${this._browser.options.name === "firefox"});
      return script.collect(${indexedDB});
    })()`;
    for (const page of this.pages()) {
      const origin = page.mainFrame().origin();
      if (!origin || !originsToSave.has(origin))
        continue;
      try {
        const storage = await page.mainFrame().nonStallingEvaluateInExistingContext(collectScript, "utility");
        if (storage.localStorage.length || storage.indexedDB?.length)
          result.origins.push({ origin, localStorage: storage.localStorage, indexedDB: storage.indexedDB });
        originsToSave.delete(origin);
      } catch {
      }
    }
    if (originsToSave.size) {
      const internalMetadata = (0, import_instrumentation.serverSideCallMetadata)();
      const page = await this.newPage(internalMetadata);
      page.addRequestInterceptor((route) => {
        route.fulfill({ body: "<html></html>" }).catch(() => {
        });
      }, "prepend");
      for (const origin of originsToSave) {
        const frame = page.mainFrame();
        await frame.goto(internalMetadata, origin, { timeout: 0 });
        const storage = await frame.evaluateExpression(collectScript, { world: "utility" });
        if (storage.localStorage.length || storage.indexedDB?.length)
          result.origins.push({ origin, localStorage: storage.localStorage, indexedDB: storage.indexedDB });
      }
      await page.close(internalMetadata);
    }
    return result;
  }
  async _resetStorage() {
    const oldOrigins = this._origins;
    const newOrigins = new Map(this._options.storageState?.origins?.map((p) => [p.origin, p]) || []);
    if (!oldOrigins.size && !newOrigins.size)
      return;
    let page = this.pages()[0];
    const internalMetadata = (0, import_instrumentation.serverSideCallMetadata)();
    page = page || await this.newPage({
      ...internalMetadata,
      // Do not mark this page as internal, because we will leave it for later reuse
      // as a user-visible page.
      isServerSide: false
    });
    const interceptor = (route) => {
      route.fulfill({ body: "<html></html>" }).catch(() => {
      });
    };
    await page.addRequestInterceptor(interceptor, "prepend");
    for (const origin of /* @__PURE__ */ new Set([...oldOrigins, ...newOrigins.keys()])) {
      const frame = page.mainFrame();
      await frame.goto(internalMetadata, origin, { timeout: 0 });
      await frame.resetStorageForCurrentOriginBestEffort(newOrigins.get(origin));
    }
    await page.removeRequestInterceptor(interceptor);
    this._origins = /* @__PURE__ */ new Set([...newOrigins.keys()]);
  }
  async _resetCookies() {
    await this.doClearCookies();
    if (this._options.storageState?.cookies)
      await this.addCookies(this._options.storageState?.cookies);
  }
  isSettingStorageState() {
    return this._settingStorageState;
  }
  async setStorageState(metadata, state) {
    this._settingStorageState = true;
    try {
      if (state.cookies)
        await this.addCookies(state.cookies);
      if (state.origins && state.origins.length) {
        const internalMetadata = (0, import_instrumentation.serverSideCallMetadata)();
        const page = await this.newPage(internalMetadata);
        await page.addRequestInterceptor((route) => {
          route.fulfill({ body: "<html></html>" }).catch(() => {
          });
        }, "prepend");
        for (const originState of state.origins) {
          const frame = page.mainFrame();
          await frame.goto(metadata, originState.origin, { timeout: 0 });
          const restoreScript = `(() => {
            const module = {};
            ${rawStorageSource.source}
            const script = new (module.exports.StorageScript())(${this._browser.options.name === "firefox"});
            return script.restore(${JSON.stringify(originState)});
          })()`;
          await frame.evaluateExpression(restoreScript, { world: "utility" });
        }
        await page.close(internalMetadata);
      }
    } finally {
      this._settingStorageState = false;
    }
  }
  async extendInjectedScript(source, arg) {
    const installInFrame = (frame) => frame.extendInjectedScript(source, arg).catch(() => {
    });
    const installInPage = (page) => {
      page.on(import_page2.Page.Events.InternalFrameNavigatedToNewDocument, installInFrame);
      return Promise.all(page.frames().map(installInFrame));
    };
    this.on(BrowserContext.Events.Page, installInPage);
    return Promise.all(this.pages().map(installInPage));
  }
  async safeNonStallingEvaluateInAllFrames(expression, world, options = {}) {
    await Promise.all(this.pages().map((page) => page.safeNonStallingEvaluateInAllFrames(expression, world, options)));
  }
  async _harStart(page, options) {
    const harId = (0, import_crypto.createGuid)();
    this._harRecorders.set(harId, new import_harRecorder.HarRecorder(this, page, options));
    return harId;
  }
  async _harExport(harId) {
    const recorder = this._harRecorders.get(harId || "");
    return recorder.export();
  }
  addRouteInFlight(route) {
    this._routesInFlight.add(route);
  }
  removeRouteInFlight(route) {
    this._routesInFlight.delete(route);
  }
  async notifyRoutesInFlightAboutRemovedHandler(handler) {
    await Promise.all([...this._routesInFlight].map((route) => route.removeHandler(handler)));
  }
}
function validateBrowserContextOptions(options, browserOptions) {
  if (options.noDefaultViewport && options.deviceScaleFactor !== void 0)
    throw new Error(`"deviceScaleFactor" option is not supported with null "viewport"`);
  if (options.noDefaultViewport && !!options.isMobile)
    throw new Error(`"isMobile" option is not supported with null "viewport"`);
  if (options.acceptDownloads === void 0 && browserOptions.name !== "electron")
    options.acceptDownloads = "accept";
  else if (options.acceptDownloads === void 0 && browserOptions.name === "electron")
    options.acceptDownloads = "internal-browser-default";
  if (!options.viewport && !options.noDefaultViewport)
    options.viewport = { width: 1280, height: 720 };
  if (options.recordVideo) {
    if (!options.recordVideo.size) {
      if (options.noDefaultViewport) {
        options.recordVideo.size = { width: 800, height: 600 };
      } else {
        const size = options.viewport;
        const scale = Math.min(1, 800 / Math.max(size.width, size.height));
        options.recordVideo.size = {
          width: Math.floor(size.width * scale),
          height: Math.floor(size.height * scale)
        };
      }
    }
    options.recordVideo.size.width &= ~1;
    options.recordVideo.size.height &= ~1;
  }
  if (options.proxy)
    options.proxy = normalizeProxySettings(options.proxy);
  verifyGeolocation(options.geolocation);
}
function verifyGeolocation(geolocation) {
  if (!geolocation)
    return;
  geolocation.accuracy = geolocation.accuracy || 0;
  const { longitude, latitude, accuracy } = geolocation;
  if (longitude < -180 || longitude > 180)
    throw new Error(`geolocation.longitude: precondition -180 <= LONGITUDE <= 180 failed.`);
  if (latitude < -90 || latitude > 90)
    throw new Error(`geolocation.latitude: precondition -90 <= LATITUDE <= 90 failed.`);
  if (accuracy < 0)
    throw new Error(`geolocation.accuracy: precondition 0 <= ACCURACY failed.`);
}
function verifyClientCertificates(clientCertificates) {
  if (!clientCertificates)
    return;
  for (const cert of clientCertificates) {
    if (!cert.origin)
      throw new Error(`clientCertificates.origin is required`);
    if (!cert.cert && !cert.key && !cert.passphrase && !cert.pfx)
      throw new Error("None of cert, key, passphrase or pfx is specified");
    if (cert.cert && !cert.key)
      throw new Error("cert is specified without key");
    if (!cert.cert && cert.key)
      throw new Error("key is specified without cert");
    if (cert.pfx && (cert.cert || cert.key))
      throw new Error("pfx is specified together with cert, key or passphrase");
  }
}
function normalizeProxySettings(proxy) {
  let { server, bypass } = proxy;
  let url;
  try {
    url = new URL(server);
    if (!url.host || !url.protocol)
      url = new URL("http://" + server);
  } catch (e) {
    url = new URL("http://" + server);
  }
  if (url.protocol === "socks4:" && (proxy.username || proxy.password))
    throw new Error(`Socks4 proxy protocol does not support authentication`);
  if (url.protocol === "socks5:" && (proxy.username || proxy.password))
    throw new Error(`Browser does not support socks5 proxy authentication`);
  server = url.protocol + "//" + url.host;
  if (bypass)
    bypass = bypass.split(",").map((t) => t.trim()).join(",");
  return { ...proxy, server, bypass };
}
const paramsThatAllowContextReuse = [
  "colorScheme",
  "forcedColors",
  "reducedMotion",
  "contrast",
  "screen",
  "userAgent",
  "viewport",
  "testIdAttributeName"
];
const defaultNewContextParamValues = {
  noDefaultViewport: false,
  ignoreHTTPSErrors: false,
  javaScriptEnabled: true,
  bypassCSP: false,
  offline: false,
  isMobile: false,
  hasTouch: false,
  acceptDownloads: "accept",
  strictSelectors: false,
  serviceWorkers: "allow",
  locale: "en-US"
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  BrowserContext,
  normalizeProxySettings,
  validateBrowserContextOptions,
  verifyClientCertificates,
  verifyGeolocation
});
