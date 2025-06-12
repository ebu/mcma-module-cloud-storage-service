import { Locator } from "@mcma/core";

export interface SourceFile {
    locator: Locator;
    egressUrl?: string;
}

export interface DestinationFile {
    locator: Locator;
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
        uploadId?: string
        segment?: MultipartSegment
        segments?: MultipartSegment[]
    };
}

export interface ActiveWorkItem {
    workItem: WorkItem;
    promise: Promise<any>;
    result?: any;
    error?: any;
}
