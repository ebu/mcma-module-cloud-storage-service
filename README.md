# mcma-module-cloud-storage-service
Service that is able to copy / move / delete files and folders between / from cloud storage locations


## Local verification

Requires Node.js 22 or newer. CI checks Linux on Node 22, 24, and 26, and Windows on Node 22.

Run from the repository root:

```sh
npm ci
npm run verify
```

The compiler is pinned to TypeScript 7.0.2. Run root `npm ci` before using Gradle too; Gradle uses the same local compiler.
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

## Storage configuration in 3.0.0

The AWS and Azure modules replace `aws_s3_buckets` and `azure_storage_accounts` with one required `storage_locations` object. Only this object's fields and nested fields change to camelCase; other Terraform input and output names remain unchanged.

```hcl
storage_locations = {
  awsS3Buckets = [{
    bucket = "example-bucket"
    region = "eu-west-1"
    # Optional: accessKey, secretKey, endpoint
  }]
  azureStorageAccounts = [{
    account          = "examplestorage"
    connectionString = var.storage_connection_string
  }]
}
```

Both lists are required; use `[]` for an unused storage type. Within bucket entries, rename `access_key` / `secret_key` to `accessKey` / `secretKey`. Within Azure account entries, rename `connection_string` to `connectionString`. `bucket`, `region`, `endpoint`, and `account` are unchanged. Old input names are removed; callers must migrate before upgrading.

The example deployment's `storage_locations` output uses the same camelCase contents. Integration scripts have been updated to read them; refresh Terraform outputs after deploying before running those scripts. Runtime secrets retain their existing `aws` / `azure` maps and camelCase values. Resource addresses, other configuration names, and both Azure plan options are unchanged.

## Build toolchain

The build uses Gradle 9.8.2 (Java 17 or newer), TypeScript 7.0.2, and esbuild 0.28.2. Direct npm dependencies are pinned and lockfiles are committed. Gradle uses `npm ci` before installing local library tarballs and checks installed versions against the lockfile afterward. The local Gradle library dependency mechanism is preserved.

Node.js 22 remains the minimum for local builds and verification. Node type definitions stay on the latest 22.x version so compilation does not assume APIs available only in newer runtimes. TypeScript's previous strictness settings are explicit, and source roots preserve the existing package entry-point layouts.

## Releases

`release-version.txt` is the release version (currently 3.0.0). The **Prepare release** GitHub Actions workflow runs from `main`. Leave **create_draft** unchecked for a dry run, or check it to create a draft after verification. Review the draft under GitHub Releases and publish it manually. Existing release tags are never replaced.

Each release contains `terraform-aws.zip`, `terraform-azure.zip`, `SHA256SUMS`, and `release.json`. The Terraform archives include freshly built deployment bundles, so consumers do not need to build JavaScript locally. CI checks archive contents, checksums, and a Terraform consumer using the extracted archives.

For local packaging, run `./gradlew prepareRelease -PreleaseBuild`; this excludes deployment and integration-script configuration and needs no cloud credentials. Run `python scripts/verify-release.py` with Terraform available to validate the archives. Verification downloads public modules and providers without accessing deployment state.

CI runs on pull requests and pushes to `main`, avoiding duplicate branch-push runs. It can also be started manually.
