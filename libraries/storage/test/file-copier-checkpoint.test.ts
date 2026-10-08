import assert from "node:assert/strict";
import { test } from "node:test";

import { CopyObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { S3Locator } from "@mcma/aws-s3";

import { FileCopier, FileCopierConfig, FileCopierState } from "../src/lib/file-copier";
import { SourceMethod, WorkItem, WorkType } from "../src/lib/model";
import { UrlTrie } from "../src/lib/url-trie";

interface Deferred<T> {
    promise: Promise<T>;
    resolve: (value: T) => void;
}

function deferred<T>(): Deferred<T> {
    let resolve: (value: T) => void;
    const promise = new Promise<T>(res => resolve = res);
    return { promise, resolve: resolve! };
}

function sleep(milliseconds: number) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

function buildSingleWorkItem(): WorkItem {
    return {
        type: WorkType.Single,
        sourceFile: {
            locator: new S3Locator({ url: "https://source-bucket.s3.us-east-1.amazonaws.com/source.bin" }),
        },
        destinationFile: {
            locator: new S3Locator({ url: "https://destination-bucket.s3.us-east-1.amazonaws.com/destination.bin" }),
        },
        sourceMethod: SourceMethod.S3Copy,
        contentLength: 1024,
        contentType: "application/octet-stream",
        retries: 0,
    };
}

function buildState(workItems: WorkItem[]): FileCopierState {
    return {
        filesTotal: 1,
        filesCopied: 0,
        bytesTotal: 1024,
        bytesCopied: 0,
        workItems,
        trie: new UrlTrie(),
    };
}

function buildConfig(send: (command: unknown) => Promise<unknown>, overrides: Partial<FileCopierConfig> = {}): FileCopierConfig {
    const s3Client = { send } as unknown as S3Client;
    return {
        maxConcurrency: 2,
        pollInterval: 5,
        activeWorkGracePeriod: 10,
        getS3Client: async () => s3Client,
        getContainerClient: async () => {
            throw new Error("Azure client was not expected");
        },
        ...overrides,
    };
}

test("live checkpoint includes active work and restored work is replayed", async () => {
    const firstCopy = deferred<Record<string, never>>();
    const firstCopyStarted = deferred<void>();
    const firstCopier = new FileCopier(buildConfig(async command => {
        assert.ok(command instanceof CopyObjectCommand);
        firstCopyStarted.resolve();
        return firstCopy.promise;
    }));
    await firstCopier.setState(buildState([buildSingleWorkItem()]));

    const firstRun = firstCopier.runUntil(new Date(Date.now() + 500), new Date(Date.now() + 1000));
    await firstCopyStarted.promise;

    const checkpointPromise = firstCopier.getCheckpointState();
    firstCopy.resolve({});
    const checkpoint = await checkpointPromise;
    assert.equal(checkpoint.workItems.length, 1);
    assert.equal(checkpoint.workItems[0].type, WorkType.Single);

    await firstRun;

    let replayCount = 0;
    const restoredCopier = new FileCopier(buildConfig(async command => {
        assert.ok(command instanceof CopyObjectCommand);
        replayCount++;
        return {};
    }));
    await restoredCopier.setState(checkpoint);
    await restoredCopier.runUntil(new Date(Date.now() + 500), new Date(Date.now() + 1000));

    assert.equal(replayCount, 1);
    assert.equal((await restoredCopier.getState()).workItems.length, 0);
});

test("periodic checkpoint writes are serialized while transfers continue", async () => {
    const copy = deferred<Record<string, never>>();
    const copyStarted = deferred<void>();
    let checkpointCalls = 0;
    let concurrentWrites = 0;
    let maximumConcurrentWrites = 0;

    const copier = new FileCopier(buildConfig(async () => {
        copyStarted.resolve();
        return copy.promise;
    }, {
        checkpointInterval: 10,
        checkpointUpdate: async state => {
            checkpointCalls++;
            concurrentWrites++;
            maximumConcurrentWrites = Math.max(maximumConcurrentWrites, concurrentWrites);
            assert.equal(state.workItems.length, 1);
            await sleep(15);
            concurrentWrites--;
        },
    }));
    await copier.setState(buildState([buildSingleWorkItem()]));

    const run = copier.runUntil(new Date(Date.now() + 500), new Date(Date.now() + 1000));
    await copyStarted.promise;
    await sleep(50);
    copy.resolve({});
    await run;

    assert.ok(checkpointCalls >= 2, `Expected at least two checkpoints, got ${checkpointCalls}`);
    assert.equal(maximumConcurrentWrites, 1);
});

test("live checkpoint retains delayed multipart completion work", async () => {
    const multipartComplete: WorkItem = {
        ...buildSingleWorkItem(),
        type: WorkType.MultipartComplete,
        multipartData: {
            s3UploadId: "upload-id",
            segments: [{ partNumber: 1, start: 0, end: 1023, length: 1024 }],
        },
    };
    const copier = new FileCopier(buildConfig(async () => {
        throw new Error("No SDK request expected while a part is incomplete");
    }, {
        delayedMultipartCompleteInterval: 100,
    }));
    await copier.setState(buildState([multipartComplete]));

    const run = copier.runUntil(new Date(Date.now() + 50), new Date(Date.now() + 500));
    await sleep(20);
    const checkpoint = await copier.getCheckpointState();

    assert.equal(checkpoint.workItems.length, 1);
    assert.equal(checkpoint.workItems[0].type, WorkType.MultipartComplete);
    await run;
});

test("final handoff aborts and requeues active work before bailout", async () => {
    const copyStarted = deferred<void>();
    const copier = new FileCopier(buildConfig(async () => {
        copyStarted.resolve();
        return new Promise(() => undefined);
    }));
    await copier.setState(buildState([buildSingleWorkItem()]));

    const startedAt = Date.now();
    const run = copier.runUntil(new Date(startedAt + 30), new Date(startedAt + 500));
    await copyStarted.promise;
    await run;

    const state = await copier.getState();
    assert.equal(state.workItems.length, 1);
    assert.equal(state.workItems[0].type, WorkType.Single);
    assert.ok(Date.now() - startedAt < 300, "Shutdown should not wait for the bailout deadline");
});
