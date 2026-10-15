# mcma-module-cloud-storage-service
Service that is able to copy / move / delete files and folders between / from cloud storage locations


## Local verification

Requires Node.js 22 or newer. CI checks Linux on Node 22, 24, and 26, and Windows on Node 22.

Run from the repository root:

```sh
npm ci
npm run verify
```

The compiler is pinned to TypeScript 5.9.3. Run root `npm ci` before using Gradle too; Gradle uses the same local compiler.
Verification reinstalls package dependencies with `npm ci`, compiles the packages, and runs local tests. It replaces generated build output and installed copies of local packages. It does not update lockfiles, publish packages, deploy services, or require cloud credentials. Runtime dependency versions are retained. The application and local libraries now emit ESM using TypeScript’s fixed `Node20` module mode.

The existing storage checkpoint tests run without cloud access. Packaging checks load staged AWS and Azure entry points as ESM using synthetic configuration and verify local libraries; they do not start the Azure Functions host or invoke cloud handlers. Real-cloud recovery scripts remain explicit integration tests and are not run by this command.

GitHub Actions uses standard hosted runners with read-only repository permissions. No cloud secrets are configured. A local pass validates only the Node/OS combination used locally; the other combinations are verified by CI after pushing.

## ESM packaging

Every application and local library declares `"type": "module"`. Lambda ZIPs include the application `package.json` alongside `index.js`; Azure ZIPs retain their `package.json` and `src/index.js` entry point. Relative imports include explicit `.js` filenames. Gradle still builds local libraries through `npmPack` task dependencies and installs their tarballs before compiling consumers.

## Temporary integration with unpublished MCMA ESM packages

First run npm ci and npm run verify in the ESM MCMA Libraries checkout. Set MCMA_LIBRARIES_ROOT to its absolute path before running this application's npm run verify or Gradle build. Gradle also accepts -PmcmaLibrariesRoot=/path/to/mcma-libraries.

The temporary bridge packs the required MCMA libraries and installs their tarballs alongside the existing Gradle npmPack dependencies. It temporarily adjusts MCMA versions during installation and in generated local-library manifests, restoring source manifests afterward and leaving lockfiles unchanged. CI checks out the reviewed library commit explicitly. No npm publication is involved.

Remove the separate temporary-integration commit after publishing the ESM libraries and updating the application's regular MCMA dependencies and lockfiles. Keep the ESM source and packaging changes.
