import { DocumentDatabaseTableProvider, getTableName, Query } from "@mcma/data";

import { StorageClientFactory, RestoreWorkItem, getRestoreWorkItemPath } from "@local/storage";
import { isS3Locator } from "@mcma/aws-s3";
import { JobAssignment, Logger } from "@mcma/core";
import { HeadObjectCommand } from "@aws-sdk/client-s3";
import { getWorkerFunctionId, WorkerInvoker } from "@mcma/worker-invoker";

import { logError, parseRestoreValue } from "@local/storage";

export interface MonitorConfig {
    dbTableProvider: DocumentDatabaseTableProvider;
    storageClientFactory: StorageClientFactory;
    workerInvoker: WorkerInvoker,
}

export class Monitor {
    constructor(private config: MonitorConfig) {

    }

    async run(logger: Logger) {
        const table = await this.config.dbTableProvider.get(getTableName());

        const query: Query<RestoreWorkItem> = {
            path: getRestoreWorkItemPath()
        };

        const restoreWorkItems: RestoreWorkItem[] = [];

        do {
            const queryResults = await table.query<RestoreWorkItem>(query);
            restoreWorkItems.push(...queryResults.results);
            query.pageStartToken = queryResults.nextPageStartToken;
        } while (query.pageStartToken);

        const jobAssignmentDatabaseIds = new Set<string>();

        for (let i = restoreWorkItems.length - 1; i >= 0; i--) {
            const restoreWorkItem = restoreWorkItems[i];

            if (isS3Locator(restoreWorkItem.file)) {
                const file = restoreWorkItem.file;

                const s3Client = await this.config.storageClientFactory.getS3Client(file.bucket, file.region);
                const headObject = await s3Client.send(new HeadObjectCommand({
                    Bucket: file.bucket,
                    Key: file.key,
                }));

                logger.info(headObject);

                const restore = parseRestoreValue(headObject.Restore);
                if (restore.get("ongoing-request") !== "true") {
                    restoreWorkItem.jobAssignmentDatabaseIds.forEach(id => jobAssignmentDatabaseIds.add(id));

                    await table.delete(restoreWorkItem.id);

                    restoreWorkItems.splice(i, 1);
                }
            } else {
                logger.error(`Detected unprocessable locator of type ${restoreWorkItem.file["@type"]}`);
                await table.delete(restoreWorkItem.id);
            }
        }

        for (const jobAssignmentDatabaseId of jobAssignmentDatabaseIds) {
            try {
                const jobCompleted = !restoreWorkItems.find(rwi => rwi.jobAssignmentDatabaseIds.includes(jobAssignmentDatabaseId));
                if (jobCompleted) {
                    const jobAssignment = await table.get<JobAssignment>(jobAssignmentDatabaseId);

                    await this.config.workerInvoker.invoke(getWorkerFunctionId(), {
                        operationName: "CompleteRestore",
                        input: {
                            jobAssignmentDatabaseId
                        },
                        tracker: jobAssignment.tracker
                    });
                }
            } catch (error) {
                logError(logger, error);
            }
        }
    }
}
