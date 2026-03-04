import { Logger, McmaException, Utils } from "@mcma/core";

export function logError(logger: Logger, error: any) {
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

function isRetryableAwsError(err: any): boolean {
    const status = err?.$metadata?.httpStatusCode;
    const code = err?.name ?? err?.code;

    if (status === 429 || status === 500 || status === 502 || status === 503 || status === 504) {
        return true;
    }

    return code === "Throttling" ||
           code === "ThrottlingException" ||
           code === "SlowDown" ||
           code === "RequestTimeout" ||
           code === "TimeoutError" ||
           code === "InternalError" ||
           code === "ServiceUnavailable";
}

export async function withRetry<T>(func: () => Promise<T>, abortSignal?: AbortSignal): Promise<T> {
    let doRetry = false;
    let attempt = 0;
    let result: T;
    do {
        doRetry = false;
        attempt++;

        try {
            result = await func();
        } catch (error) {
            if (abortSignal?.aborted) {
                throw error;
            }

            if (!isRetryableAwsError(error) || attempt > 2) {
                throw error;
            }

            doRetry = true;
            await Utils.sleep(3000);
        }
    } while (doRetry);

    return result;
}
