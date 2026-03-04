import { Readable, Writable, PassThrough } from "stream";
import { once } from "events";
import * as readline from "readline";
import { createGzip, createGunzip } from "zlib";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { BlockBlobClient } from "@azure/storage-blob";

/***********
 * TrieNode
 ***********/

class TrieNode {
    children: { [key: string]: TrieNode } = Object.create(null);
    isEnd = false;
}

/***********
 * UrlTrie
 ***********/

export class UrlTrie {
    private root = new TrieNode();

    private insertKey(key: string): boolean {
        const parts = key.split("/").filter(Boolean);
        let node = this.root;
        for (const part of parts) {
            if (!node.children[part]) {
                node.children[part] = new TrieNode();
            }
            node = node.children[part];
        }
        const alreadyExists = node.isEnd;
        node.isEnd = true;
        return !alreadyExists;
    }

    insert(url: string): boolean {
        return this.insertKey(normalizeUrlForTrieKey(url));
    }

    has(url: string): boolean {
        const parts = normalizeUrlForTrieKey(url).split("/").filter(Boolean);
        let node = this.root;
        for (const part of parts) {
            if (!node.children[part]) {
                return false;
            }
            node = node.children[part];
        }
        return node.isEnd;
    }

    // Write paths one per line to a writable stream — constant memory
    async serialize(stream: Writable): Promise<void> {
        const writeNode = async (node: TrieNode, parts: string[]): Promise<void> => {
            if (node.isEnd) {
                await writeOrDrain(stream, parts.join("/") + "\n");
            }
            // iterate keys in stable order if desired
            for (const key of Object.keys(node.children).sort()) {
                await writeNode(node.children[key], [...parts, key]);
            }
        };
        await writeNode(this.root, []);
    }

    // Read line by line and rebuild trie — constant memory
    static async deserialize(stream: Readable): Promise<UrlTrie> {
        const trie = new UrlTrie();
        const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

        for await (const line of rl) {
            if (line.trim()) {
                trie.insertKey(line.trim());
            }
        }

        return trie;
    }

    async clone(): Promise<UrlTrie> {
        const passthrough = new PassThrough();
        // start deserialization immediately so we stream data instead of buffering whole serialize
        const deserPromise = UrlTrie.deserialize(passthrough);
        // serialize and then end the passthrough
        try {
            await this.serialize(passthrough);
            passthrough.end();
            return await deserPromise;
        } catch (err) {
            passthrough.destroy(err as Error);
            throw err;
        }
    }
}

/***********
 * Functions
 ***********/

function normalizeUrlForTrieKey(rawUrl: string): string {
    const u = new URL(rawUrl);
    const host = u.hostname.toLowerCase();

    let path = u.pathname || "/";
    if (path !== "/" && path.endsWith("/")) {
        path = path.slice(0, -1);
    }

    return host + path + (u.search || "");
}

async function writeOrDrain(stream: Writable, chunk: string | Buffer): Promise<void> {
    if (!stream.write(chunk)) {
        // wait for drain
        await once(stream, "drain");
    }
}

async function streamToBuffer(stream: Readable): Promise<Buffer> {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk) => {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    await once(stream, "end");
    return Buffer.concat(chunks);
}

export async function saveTrieToS3(trie: UrlTrie, s3: S3Client, bucket: string, key: string): Promise<void> {
    const passthrough = new PassThrough();
    const gzip = createGzip();
    passthrough.pipe(gzip);

    try {
        // Start producing data
        const gzippedPromise = streamToBuffer(gzip as unknown as Readable);

        await trie.serialize(passthrough);
        passthrough.end();

        const gzipped = await gzippedPromise;

        await s3.send(new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: gzipped,
            ContentEncoding: "gzip",
            ContentLength: gzipped.length,
        }));
    } catch (err) {
        passthrough.destroy(err as Error);
        gzip.destroy(err as Error);
        throw err;
    }
}

export async function loadTrieFromS3(s3: S3Client, bucket: string, key: string): Promise<UrlTrie> {
    try {
        const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        const body = response.Body;

        if (!body) {
            return new UrlTrie();
        }

        const reader = body as Readable;
        const gunzip = createGunzip();
        reader.pipe(gunzip);

        return UrlTrie.deserialize(gunzip);
    } catch (error: any) {
        const code = error?.name ?? error?.Code ?? error?.code;
        const status = error?.$metadata?.httpStatusCode;

        if (code === "NoSuchKey" || status === 404) {
            return new UrlTrie();
        }

        throw error;
    }
}

export async function saveTrieToBlobStorage(trie: UrlTrie, blobClient: BlockBlobClient): Promise<void> {
    const passthrough = new PassThrough();
    const gzip = createGzip();
    passthrough.pipe(gzip);

    // Azure SDK can upload from a Node Readable stream.
    const uploadPromise = blobClient.uploadStream(gzip, undefined, undefined, {
        blobHTTPHeaders: {
            blobContentEncoding: "gzip",
            blobContentType: "text/plain",
        },
    });

    try {
        await trie.serialize(passthrough);
        passthrough.end();
        await uploadPromise;
    } catch (err) {
        passthrough.destroy(err as Error);
        gzip.destroy(err as Error);
        throw err;
    }
}

export async function loadTrieFromBlobStorage(blobClient: BlockBlobClient): Promise<UrlTrie> {
    try {
        const downloadResponse = await blobClient.download();
        const readable = downloadResponse.readableStreamBody;

        if (!readable) {
            return new UrlTrie();
        }

        const reader = readable as unknown as Readable;
        const gunzip = createGunzip();
        reader.pipe(gunzip);

        return UrlTrie.deserialize(gunzip);
    } catch (error: any) {
        if (error?.statusCode === 404) {
            return new UrlTrie();
        }

        throw error;
    }
}
