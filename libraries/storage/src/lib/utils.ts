import { Logger, McmaException } from "@mcma/core";

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
