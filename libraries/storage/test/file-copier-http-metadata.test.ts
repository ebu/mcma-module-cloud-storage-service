import assert from "node:assert/strict";
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { AddressInfo } from "node:net";
import { test } from "node:test";

import { Locator } from "@mcma/core";

import { FileCopier, FileCopierConfig } from "../src/lib/file-copier.js";
import { WorkItem, WorkType } from "../src/lib/model.js";

interface HttpRequest {
    method?: string;
    range?: string;
}

interface ScanResult {
    contentLength: number;
    contentType?: string;
    lastModified?: Date;
}

type ScanFile = (workItem: WorkItem, abortSignal: AbortSignal) => Promise<ScanResult>;

function buildCopier() {
    const config: FileCopierConfig = {
        getS3Client: async () => {
            throw new Error("S3 client was not expected");
        },
        getContainerClient: async () => {
            throw new Error("Azure client was not expected");
        },
    };
    return new FileCopier(config);
}

function buildScanWorkItem(sourceUrl: string): WorkItem {
    return {
        type: WorkType.ScanFile,
        sourceFile: {
            locator: new Locator({ url: sourceUrl }),
        },
        destinationFile: {
            locator: new Locator({ url: "https://destination.example/file.bin" }),
        },
        retries: 0,
    };
}

async function scanHttpFile(sourceUrl: string) {
    const copier = buildCopier();
    const scanFile = (copier as unknown as { processWorkItemScanFile: ScanFile }).processWorkItemScanFile.bind(copier);
    return scanFile(buildScanWorkItem(sourceUrl), new AbortController().signal);
}

async function withHttpServer(
    handler: (request: IncomingMessage, response: ServerResponse) => void,
    action: (url: string) => Promise<void>
) {
    const server = createServer(handler);
    await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
    });

    try {
        const address = server.address() as AddressInfo;
        await action(`http://127.0.0.1:${address.port}/file.bin`);
    } finally {
        await new Promise<void>((resolve, reject) => {
            server.close(error => error ? reject(error) : resolve());
        });
    }
}

function recordRequest(requests: HttpRequest[], request: IncomingMessage) {
    requests.push({
        method: request.method,
        range: request.headers.range,
    });
}

test("uses a valid Content-Length from HEAD without a range request", async () => {
    const requests: HttpRequest[] = [];
    await withHttpServer((request, response) => {
        recordRequest(requests, request);
        response.writeHead(200, {
            "Content-Length": "1234",
            "Content-Type": "application/octet-stream",
            "Last-Modified": "Tue, 20 Oct 2026 12:00:00 GMT",
        });
        response.end();
    }, async url => {
        const result = await scanHttpFile(url);
        assert.equal(result.contentLength, 1234);
        assert.equal(result.contentType, "application/octet-stream");
        assert.equal(result.lastModified?.toISOString(), "2026-10-20T12:00:00.000Z");
    });

    assert.deepEqual(requests, [{ method: "HEAD", range: undefined }]);
});

test("uses Content-Range when HEAD incorrectly reports zero", async () => {
    const requests: HttpRequest[] = [];
    await withHttpServer((request, response) => {
        recordRequest(requests, request);
        if (request.method === "HEAD") {
            response.writeHead(200, { "Content-Length": "0" });
        } else {
            response.writeHead(206, {
                "Content-Length": "1",
                "Content-Range": "bytes 0-0/517140624",
            });
        }
        response.end();
    }, async url => {
        const result = await scanHttpFile(url);
        assert.equal(result.contentLength, 517140624);
    });

    assert.deepEqual(requests, [
        { method: "HEAD", range: undefined },
        { method: "GET", range: "bytes=0-0" },
    ]);
});

test("preserves a genuine zero-byte file reported by a 416 range response", async () => {
    const requests: HttpRequest[] = [];
    await withHttpServer((request, response) => {
        recordRequest(requests, request);
        if (request.method === "HEAD") {
            response.writeHead(200, { "Content-Length": "0" });
        } else {
            response.writeHead(416, { "Content-Range": "bytes */0" });
        }
        response.end();
    }, async url => {
        const result = await scanHttpFile(url);
        assert.equal(result.contentLength, 0);
    });

    assert.deepEqual(requests, [
        { method: "HEAD", range: undefined },
        { method: "GET", range: "bytes=0-0" },
    ]);
});

test("uses Content-Range when HEAD omits Content-Length", async () => {
    const requests: HttpRequest[] = [];
    await withHttpServer((request, response) => {
        recordRequest(requests, request);
        if (request.method === "HEAD") {
            response.writeHead(200);
        } else {
            response.writeHead(206, {
                "Content-Length": "1",
                "Content-Range": "bytes 0-0/517140624",
            });
        }
        response.end();
    }, async url => {
        const result = await scanHttpFile(url);
        assert.equal(result.contentLength, 517140624);
    });

    assert.deepEqual(requests, [
        { method: "HEAD", range: undefined },
        { method: "GET", range: "bytes=0-0" },
    ]);
});

test("rejects malformed Content-Range metadata", async () => {
    await withHttpServer((request, response) => {
        if (request.method === "HEAD") {
            response.writeHead(200, { "Content-Length": "0" });
        } else {
            response.writeHead(206, {
                "Content-Length": "1",
                "Content-Range": "bytes 0-1/517140624",
            });
        }
        response.end();
    }, async url => {
        await assert.rejects(scanHttpFile(url), /Invalid Content-Range/);
    });
});
