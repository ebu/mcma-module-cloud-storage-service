import { DocumentDatabaseTable } from "@mcma/data";
import { FileCopierState } from "./file-copier";
import { WorkItem } from "./model";
import { Logger } from "@mcma/core";

// saving max 200 work items per database entry
const STATE_DATABASE_ENTRY_SIZE = 200;

function computePrefixDatabaseId(jobAssignmentDatabaseId: string) {
    return jobAssignmentDatabaseId + "/file-copier-state-";
}

function computeIndexDatabaseId(jobAssignmentDatabaseId: string) {
    return computePrefixDatabaseId(jobAssignmentDatabaseId) + "index";
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

        await table.put(databaseId, item);

        databaseIds.push(databaseId);
    }

    const indexDatabaseId = computeIndexDatabaseId(jobAssignmentDatabaseId);

    await table.put(indexDatabaseId, { databaseIds: databaseIds });
}

export async function loadFileCopierState(jobAssignmentDatabaseId: string, table: DocumentDatabaseTable): Promise<FileCopierState> {
    let bytesTotal: number = 0;
    let bytesCopied: number = 0;
    let filesTotal: number = 0;
    let filesCopied: number = 0;
    let workItems: WorkItem[] = [];

    const indexDatabaseId = computeIndexDatabaseId(jobAssignmentDatabaseId);

    const index = await table.get<{ databaseIds: string[]}>(indexDatabaseId);

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
        await table.delete(indexDatabaseId);

        for (const fileCopierStateDatabaseId of index.databaseIds) {
            await table.delete(fileCopierStateDatabaseId);
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
