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
var localUtils_exports = {};
__export(localUtils_exports, {
  addStackToTracingNoReply: () => addStackToTracingNoReply,
  harClose: () => harClose,
  harLookup: () => harLookup,
  harOpen: () => harOpen,
  harUnzip: () => harUnzip,
  traceDiscarded: () => traceDiscarded,
  tracingStarted: () => tracingStarted,
  zip: () => zip
});
module.exports = __toCommonJS(localUtils_exports);
var import_fs = __toESM(require("fs"));
var import_os = __toESM(require("os"));
var import_path = __toESM(require("path"));
var import_crypto = require("./utils/crypto");
var import_harBackend = require("./harBackend");
var import_manualPromise = require("../utils/isomorphic/manualPromise");
var import_zipFile = require("./utils/zipFile");
var import_zipBundle = require("../zipBundle");
var import_traceUtils = require("../utils/isomorphic/traceUtils");
var import_assert = require("../utils/isomorphic/assert");
var import_fileUtils = require("./utils/fileUtils");
async function zip(stackSessions, params) {
  const promise = new import_manualPromise.ManualPromise();
  const zipFile = new import_zipBundle.yazl.ZipFile();
  zipFile.on("error", (error) => promise.reject(error));
  const addFile = (file, name) => {
    try {
      if (import_fs.default.statSync(file).isFile())
        zipFile.addFile(file, name);
    } catch (e) {
    }
  };
  for (const entry of params.entries)
    addFile(entry.value, entry.name);
  const stackSession = params.stacksId ? stackSessions.get(params.stacksId) : void 0;
  if (stackSession?.callStacks.length) {
    await stackSession.writer;
    if (process.env.PW_LIVE_TRACE_STACKS) {
      zipFile.addFile(stackSession.file, "trace.stacks");
    } else {
      const buffer = Buffer.from(JSON.stringify((0, import_traceUtils.serializeClientSideCallMetadata)(stackSession.callStacks)));
      zipFile.addBuffer(buffer, "trace.stacks");
    }
  }
  if (params.includeSources) {
    const sourceFiles = /* @__PURE__ */ new Set();
    for (const { stack } of stackSession?.callStacks || []) {
      if (!stack)
        continue;
      for (const { file } of stack)
        sourceFiles.add(file);
    }
    for (const sourceFile of sourceFiles)
      addFile(sourceFile, "resources/src@" + await (0, import_crypto.calculateSha1)(sourceFile) + ".txt");
  }
  if (params.mode === "write") {
    await import_fs.default.promises.mkdir(import_path.default.dirname(params.zipFile), { recursive: true });
    zipFile.end(void 0, () => {
      zipFile.outputStream.pipe(import_fs.default.createWriteStream(params.zipFile)).on("close", () => promise.resolve()).on("error", (error) => promise.reject(error));
    });
    await promise;
    await deleteStackSession(stackSessions, params.stacksId);
    return;
  }
  const tempFile = params.zipFile + ".tmp";
  await import_fs.default.promises.rename(params.zipFile, tempFile);
  import_zipBundle.yauzl.open(tempFile, (err, inZipFile) => {
    if (err) {
      promise.reject(err);
      return;
    }
    (0, import_assert.assert)(inZipFile);
    let pendingEntries = inZipFile.entryCount;
    inZipFile.on("entry", (entry) => {
      inZipFile.openReadStream(entry, (err2, readStream) => {
        if (err2) {
          promise.reject(err2);
          return;
        }
        zipFile.addReadStream(readStream, entry.fileName);
        if (--pendingEntries === 0) {
          zipFile.end(void 0, () => {
            zipFile.outputStream.pipe(import_fs.default.createWriteStream(params.zipFile)).on("close", () => {
              import_fs.default.promises.unlink(tempFile).then(() => {
                promise.resolve();
              }).catch((error) => promise.reject(error));
            });
          });
        }
      });
    });
  });
  await promise;
  await deleteStackSession(stackSessions, params.stacksId);
}
async function deleteStackSession(stackSessions, stacksId) {
  const session = stacksId ? stackSessions.get(stacksId) : void 0;
  if (!session)
    return;
  await session.writer;
  if (session.tmpDir)
    await (0, import_fileUtils.removeFolders)([session.tmpDir]);
  stackSessions.delete(stacksId);
}
async function harOpen(harBackends, params) {
  let harBackend;
  if (params.file.endsWith(".zip")) {
    const zipFile = new import_zipFile.ZipFile(params.file);
    const entryNames = await zipFile.entries();
    const harEntryName = entryNames.find((e) => e.endsWith(".har"));
    if (!harEntryName)
      return { error: "Specified archive does not have a .har file" };
    const har = await zipFile.read(harEntryName);
    const harFile = JSON.parse(har.toString());
    harBackend = new import_harBackend.HarBackend(harFile, null, zipFile);
  } else {
    const harFile = JSON.parse(await import_fs.default.promises.readFile(params.file, "utf-8"));
    harBackend = new import_harBackend.HarBackend(harFile, import_path.default.dirname(params.file), null);
  }
  harBackends.set(harBackend.id, harBackend);
  return { harId: harBackend.id };
}
async function harLookup(harBackends, params) {
  const harBackend = harBackends.get(params.harId);
  if (!harBackend)
    return { action: "error", message: `Internal error: har was not opened` };
  return await harBackend.lookup(params.url, params.method, params.headers, params.postData, params.isNavigationRequest);
}
async function harClose(harBackends, params) {
  const harBackend = harBackends.get(params.harId);
  if (harBackend) {
    harBackends.delete(harBackend.id);
    harBackend.dispose();
  }
}
async function harUnzip(params) {
  const dir = import_path.default.dirname(params.zipFile);
  const zipFile = new import_zipFile.ZipFile(params.zipFile);
  for (const entry of await zipFile.entries()) {
    const buffer = await zipFile.read(entry);
    if (entry === "har.har")
      await import_fs.default.promises.writeFile(params.harFile, buffer);
    else
      await import_fs.default.promises.writeFile(import_path.default.join(dir, entry), buffer);
  }
  zipFile.close();
  await import_fs.default.promises.unlink(params.zipFile);
}
async function tracingStarted(stackSessions, params) {
  let tmpDir = void 0;
  if (!params.tracesDir)
    tmpDir = await import_fs.default.promises.mkdtemp(import_path.default.join(import_os.default.tmpdir(), "playwright-tracing-"));
  const traceStacksFile = import_path.default.join(params.tracesDir || tmpDir, params.traceName + ".stacks");
  stackSessions.set(traceStacksFile, { callStacks: [], file: traceStacksFile, writer: Promise.resolve(), tmpDir });
  return { stacksId: traceStacksFile };
}
async function traceDiscarded(stackSessions, params) {
  await deleteStackSession(stackSessions, params.stacksId);
}
async function addStackToTracingNoReply(stackSessions, params) {
  for (const session of stackSessions.values()) {
    session.callStacks.push(params.callData);
    if (process.env.PW_LIVE_TRACE_STACKS) {
      session.writer = session.writer.then(() => {
        const buffer = Buffer.from(JSON.stringify((0, import_traceUtils.serializeClientSideCallMetadata)(session.callStacks)));
        return import_fs.default.promises.writeFile(session.file, buffer);
      });
    }
  }
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  addStackToTracingNoReply,
  harClose,
  harLookup,
  harOpen,
  harUnzip,
  traceDiscarded,
  tracingStarted,
  zip
});
