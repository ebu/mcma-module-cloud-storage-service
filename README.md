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
Verification reinstalls package dependencies with `npm ci`, compiles the packages, and runs local tests. It replaces generated build output and installed copies of local packages. It does not update lockfiles, publish packages, deploy services, or require cloud credentials. Runtime dependencies and CommonJS output are retained.

The existing storage checkpoint tests run without cloud access. Packaging checks validate staged application entry points and local libraries; they do not start the Azure Functions host or invoke cloud handlers. Real-cloud recovery scripts remain explicit integration tests and are not run by this command.

GitHub Actions uses standard hosted runners with read-only repository permissions. No cloud secrets are configured. A local pass validates only the Node/OS combination used locally; the other combinations are verified by CI after pushing.
