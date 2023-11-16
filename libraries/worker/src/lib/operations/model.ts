import { Locator } from "@mcma/core";

export interface SourceFile {
    locator: Locator,
    alternateUrl?: string,
    alternateAuthType?: string,
}

export interface TargetFile {
    locator: Locator,
}

export interface ObjectData {
    contentType?: string;
    etag?: string;
    size?: number;
    lastModified?: Date;
}
