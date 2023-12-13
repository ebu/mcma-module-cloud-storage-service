import { DocumentDatabaseTable } from "@mcma/data";
import { FileCopierState } from "./file-copier";
import { WorkItem } from "./model";

export async function saveFileCopierState(state: FileCopierState, jobAssignmentDatabaseId: string, table: DocumentDatabaseTable): Promise<string[]> {
    const fileCopierStateDatabaseIds: string[] = [];

    const fileCopierStateDatabaseIdPrefix = jobAssignmentDatabaseId + "/file-copier-state-";

    // saving max 500 work items per database entry
    for (let i = 0; i < state.workItems.length; i = i + 500) {
        const fileCopierStateDatabaseId = fileCopierStateDatabaseIdPrefix + i;

        let item: any = {};
        if (i === 0) {
            item.bytesTotal = state.bytesTotal;
            item.bytesCopied = state.bytesCopied;
            item.filesTotal = state.filesTotal;
            item.filesCopied = state.filesCopied;
        }
        item.workItems = state.workItems.slice(i, i + 500);

        await table.put(fileCopierStateDatabaseId, item);

        fileCopierStateDatabaseIds.push(fileCopierStateDatabaseId);
    }

    return fileCopierStateDatabaseIds;
}

export async function loadFileCopierState(fileCopierStateDatabaseIds: string[], table: DocumentDatabaseTable): Promise<FileCopierState> {
    let bytesTotal: number = 0;
    let bytesCopied: number = 0;
    let filesTotal: number = 0;
    let filesCopied: number = 0;
    let workItems: WorkItem[] = [];

    for (const fileCopierStateDatabaseId of fileCopierStateDatabaseIds) {
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

        await table.delete(fileCopierStateDatabaseId);
    }

    return {
        bytesTotal,
        bytesCopied,
        filesTotal,
        filesCopied,
        workItems,
    };
}
