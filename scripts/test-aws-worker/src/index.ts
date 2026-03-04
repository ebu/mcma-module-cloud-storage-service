import { v4 as uuidV4 } from "uuid";

import { handler } from "../../../aws/worker/src";

function log(entry?: any) {
    if (typeof entry === "object") {
        console.log(JSON.stringify(entry, null, 2));
    } else {
        console.log(entry);
    }
}

async function main() {
    console.log("Starting");

    const event = require("./event.json");
    log({
        event
    });

    const endTime = Date.now() + 15 * 60 * 1000;

    await handler(
        {
            ...event,
            useLocalLogging: true
        },
        {
            awsRequestId: uuidV4(),
            callbackWaitsForEmptyEventLoop: false,
            functionName: "",
            functionVersion: "",
            invokedFunctionArn: "",
            logGroupName: "",
            logStreamName: "",
            memoryLimitInMB: "",
            done(error?: Error, result?: any): void {
            },
            fail(error: Error | string): void {
            },
            getRemainingTimeInMillis(): number {
                return endTime - Date.now();
            },
            succeed(messageOrObject: any, object?: any): void {
            }
        }
    );
}

main().catch(console.error).finally(() => console.log("Done"));
