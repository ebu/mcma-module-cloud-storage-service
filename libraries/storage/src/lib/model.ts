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
    ScanFolder = "ScanFolder",
    ScanFile = "ScanFile",
    ProcessFolder = "ProcessFolder",
    ProcessFile = "ProcessFile",
    Single = "Single",
    MultipartStart = "MultipartStart",
    MultipartSegment = "MultipartSegment",
    MultipartComplete = "MultipartComplete",
}

export const WorkTypePriority: Record<WorkType, number> = {
    [WorkType.ScanFile]: 1,
    [WorkType.ScanFolder]: 1,
    [WorkType.MultipartComplete]: 2,
    [WorkType.Single]: 3,
    [WorkType.MultipartStart]: 3,
    [WorkType.MultipartSegment]: 3,
    [WorkType.ProcessFile]: 4,
    [WorkType.ProcessFolder]: 5,
};

export interface MultipartSegment {
    partNumber: number;
    start: number;
    end: number;
    length: number;
    etag?: string;
    blockId?: string;
}

export enum SourceMethod {
    EgressUrl = "EgressUrl",
    LocatorUrl = "LocatorUrl",
    S3Copy = "S3Copy",
    SignedUrl = "SignedUrl",
}

export interface WorkItem {
    type: WorkType;
    sourceFile: SourceFile;
    destinationFile: DestinationFile;
    continuationToken?: string;
    sourceMethod?: SourceMethod;
    contentLength?: number;
    contentType?: string;
    lastModified?: Date;
    multipartData?: {
        nextPartNumber?: number;
        nextBytePosition?: number;
        multipartSize?: number;
        s3UploadId?: string;
        blockIdPrefix?: string;
        segment?: MultipartSegment;
        segments?: MultipartSegment[];
    };
    retries: number;
}

export interface ActiveWorkItem {
    workItem: WorkItem;
    promise: Promise<any>;
    abortController: AbortController;
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
