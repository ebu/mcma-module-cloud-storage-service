import { StorageClass } from "@aws-sdk/client-s3";
import { Locator, McmaResource, McmaResourceProperties } from "@mcma/core";

export interface SourceFile {
    locator: Locator;
    egressUrl?: string;
}

export interface DestinationFile {
    locator: Locator;
    storageClass?: StorageClass;
}

export enum WorkType {
    Prepare = "Prepare",
    Single = "Single",
    MultipartStart = "MultipartStart",
    MultipartSegment = "MultipartSegment",
    MultipartComplete = "MultipartComplete",
}

export interface MultipartSegment {
    partNumber: number;
    start: number;
    end: number;
    length: number;
    etag?: string;
    blockId?: string;
}

export interface WorkItem {
    type: WorkType;
    sourceFile: SourceFile;
    destinationFile: DestinationFile;
    retries: number;
    sourceUrl?: string;
    sourceHeaders?: { [key: string]: string };
    contentLength?: number;
    contentType?: string;
    lastModified?: Date;
    multipartData?: {
        uploadId?: string;
        segment?: MultipartSegment;
        segments?: MultipartSegment[];
        alreadyCounted?: boolean;
    };
}

export interface ActiveWorkItem {
    workItem: WorkItem;
    promise: Promise<any>;
    result?: any;
    error?: any;
}

export interface RestoreWorkItemProperties extends McmaResourceProperties {
    file: Locator,
    jobAssignmentDatabaseIds?: string[],
}

export class RestoreWorkItem extends McmaResource implements RestoreWorkItemProperties {
    file: Locator;
    jobAssignmentDatabaseIds: string[];

    constructor(properties: RestoreWorkItemProperties) {
        super("RestoreWorkItem", properties);

        this.file = properties.file;

        this.jobAssignmentDatabaseIds = [];
        if (Array.isArray(properties.jobAssignmentDatabaseIds)) {
            this.jobAssignmentDatabaseIds.push(...properties.jobAssignmentDatabaseIds);
        }
    }
}

export function getRestoreWorkItemPath() {
    return "/restore-work-items";
}

export function buildRestoreWorkItemId(locator: Locator) {
    return `${getRestoreWorkItemPath()}/${locator.url.replace(/[:/]+/g, "-")}`;
}

export enum RestorePriority {
    High = "High",
    Medium = "Medium",
    Low = "Low",
}
