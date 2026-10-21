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

The existing storage checkpoint tests run without cloud access. Packaging checks load staged AWS and Azure entry points as ESM using synthetic configuration and verify local libraries. Verification also builds deployment bundles and loads them from a temporary directory outside the checkout, so missing dependencies cannot resolve from the development tree; they do not start the Azure Functions host or invoke cloud handlers. Real-cloud recovery scripts remain explicit integration tests and are not run by this command.

GitHub Actions uses standard hosted runners with read-only repository permissions. No cloud secrets are configured. A local pass validates only the Node/OS combination used locally; the other combinations are verified by CI after pushing.

## ESM packaging

Every application and local library declares `"type": "module"`. Lambda ZIPs include the application `package.json` alongside `index.js`; Azure ZIPs retain their `package.json` and `src/index.js` entry point. Relative imports include explicit `.js` filenames. Gradle still builds local libraries through `npmPack` task dependencies and installs their tarballs before compiling consumers.

## Published MCMA libraries

MCMA dependencies are pinned to the published 2.0.0 packages from npm. No sibling MCMA Libraries checkout or `mcmaLibrariesRoot` parameter is needed. CI installs the committed lockfiles directly.

Your application-local `@local/*` libraries are still packed and installed through Gradle task dependencies. After root `npm ci`, build AWS and Azure deployment ZIPs with `./gradlew build` (Windows: `.\gradlew.bat build`). Cloud deployment and live integration tests remain separate, explicit commands.

## Deployment bundles

Gradle bundles the compiled AWS and Azure applications with pinned esbuild before creating deployment ZIPs. Tree shaking removes unused code; the full development `node_modules` directory is no longer copied. TypeScript compilation remains a separate type check, and the existing local-library `npmPack` dependencies are preserved.

Bundles remain ESM and target Node.js 22 or newer. They retain function/class names, linked source maps, and third-party license notices. Azure keeps `@azure/functions` and the identity SDK's `open` helper, including their installed runtime dependencies and assets, outside the bundle. AWS bundles require no packaged `node_modules` directory.

Run root `npm ci`, then `./gradlew build` (Windows: `.\gradlew.bat build`) as before. Each deployment project writes its bundle to `build/bundle` and an esbuild size-analysis manifest to `build/bundle-meta.json`. The manifest is not included in the deployment ZIP. Gradle checks each bundle in isolation before creating the ZIP; `npm run verify` runs the same bundle checks in CI. These checks verify startup without network access, but deployed AWS and Azure integration tests are still needed to validate runtime behavior.

## Deployment with MCMA 2.0.0 services

The example deployment consumes the public Service Registry and Job Processor 2.0.0 Terraform release archives, including their prebuilt function bundles. Cloud Storage runs Node.js 26 on AWS Lambda and Node.js 24 on Azure Functions. Both Flex Consumption and Windows/custom App Service plans remain supported.

The Azure module requires AzureRM >= 5.10.0, < 6.0.0; the AWS module requires AWS provider >= 6.69.1, < 7.0.0. AzAPI remains >= 2.13.0. Key Vault continues using access policies. The committed provider lockfile includes Windows and Linux checksums.

After pulling these changes, run `.\gradlew.bat plan` to download the new modules and locked providers and review the deployment changes. Then run `.\gradlew.bat deploy` and test a copy job end to end. Existing local Terraform state and backend configuration are retained.
