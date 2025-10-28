import { DocumentDatabaseTable } from "@mcma/data";
import { Logger, McmaException, Utils } from "@mcma/core";
import { FileCopierState } from "./file-copier";
import { WorkItem } from "./model";

// saving max 200 work items per database entry
const STATE_DATABASE_ENTRY_SIZE = 200;

function computePrefixDatabaseId(jobAssignmentDatabaseId: string) {
    return jobAssignmentDatabaseId + "/file-copier-state-";
}

function computeIndexDatabaseId(jobAssignmentDatabaseId: string) {
    return computePrefixDatabaseId(jobAssignmentDatabaseId) + "index";
}

async function withRetry<T>(func: () => Promise<T>): Promise<T> {
    let doRetry = false;
    let attempt = 0;
    let result: T;
    do {
        doRetry = false;
        attempt++;

        try {
            result = await func();
        } catch (error) {
            if (attempt > 2) {
                throw error;
            }
            doRetry = true;

            await Utils.sleep(3000);
        }
    } while (doRetry);

    return result;
}

export async function saveFileCopierState(state: FileCopierState, jobAssignmentDatabaseId: string, table: DocumentDatabaseTable): Promise<void> {
    const databaseIds: string[] = [];

    const databaseIdPrefix = computePrefixDatabaseId(jobAssignmentDatabaseId);

    for (let i = 0; i < state.workItems.length; i = i + STATE_DATABASE_ENTRY_SIZE) {
        const databaseId = databaseIdPrefix + i;

        let item: any = {};
        if (i === 0) {
            item.bytesTotal = state.bytesTotal;
            item.bytesCopied = state.bytesCopied;
            item.filesTotal = state.filesTotal;
            item.filesCopied = state.filesCopied;
        }
        item.workItems = state.workItems.slice(i, i + STATE_DATABASE_ENTRY_SIZE);

        await withRetry(async () => await table.put(databaseId, item));

        databaseIds.push(databaseId);
    }

    const indexDatabaseId = computeIndexDatabaseId(jobAssignmentDatabaseId);

    await withRetry(async () => await table.put(indexDatabaseId, { databaseIds: databaseIds }));
}

export async function loadFileCopierState(jobAssignmentDatabaseId: string, table: DocumentDatabaseTable): Promise<FileCopierState> {
    let bytesTotal: number = 0;
    let bytesCopied: number = 0;
    let filesTotal: number = 0;
    let filesCopied: number = 0;
    let workItems: WorkItem[] = [];

    const indexDatabaseId = computeIndexDatabaseId(jobAssignmentDatabaseId);

    const index = await withRetry(async () => await table.get<{ databaseIds: string[] }>(indexDatabaseId));

    for (const fileCopierStateDatabaseId of index.databaseIds) {
        const item = await table.get<FileCopierState>(fileCopierStateDatabaseId);

        if (item?.bytesTotal) {
            bytesTotal = item.bytesTotal;
        }
        if (item?.bytesCopied) {
            bytesCopied = item.bytesCopied;
        }
        if (item?.filesTotal) {
            filesTotal = item.filesTotal;
        }
        if (item?.filesCopied) {
            filesCopied = item.filesCopied;
        }
        if (Array.isArray(item?.workItems)) {
            workItems.push(...item.workItems);
        }
    }

    return {
        bytesTotal,
        bytesCopied,
        filesTotal,
        filesCopied,
        workItems,
    };
}

export async function deleteFileCopierState(jobAssignmentDatabaseId: string, table: DocumentDatabaseTable): Promise<void> {
    const indexDatabaseId = computeIndexDatabaseId(jobAssignmentDatabaseId);

    try {
        const index = await table.get<{ databaseIds: string[] }>(indexDatabaseId);
        await withRetry(async () => await table.delete(indexDatabaseId));

        for (const fileCopierStateDatabaseId of index.databaseIds) {
            await withRetry(async () => await table.delete(fileCopierStateDatabaseId));
        }
    } catch {}
}

export function logError(logger: Logger, error: Error) {
    logger?.error(error?.name);
    logger?.error(error?.message);
    logger?.error(error?.stack);
    logger?.error(error?.toString());
    logger?.error(error);
}

export function parseRestoreValue(restore: string): Map<string, string> {
    const map = new Map<string, string>();

    if (restore) {
        let state = 0;
        let start = 0;

        let key: string = "";
        let value: string = "";

        for (let i = 0; i < restore.length; i++) {
            const c = restore.charAt(i);

            switch (c) {
                case ",":
                case " ":
                    if (state === 0) {
                        start = i + 1;
                    }
                    break;
                case "=":
                    if (state === 0) {
                        key = restore.substring(start, i);
                        state = 1;
                    }
                    break;
                case "\"":
                    if (state === 1) {
                        start = i + 1;
                        state = 2;
                    } else {
                        value = restore.substring(start, i);
                        if (!key) {
                            throw new McmaException("Failed to parse Restore string");
                        }
                        map.set(key, value);
                        state = 0
                    }
                    break;
            }
        }
    }

    return map;
}
