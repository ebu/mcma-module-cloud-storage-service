import { app } from "@azure/functions";

import { apiHandler } from "./api-handler.js";
import { monitor } from "./monitor.js";
import { workerQueueHandler } from "./worker.js";

app.http("api-handler", {
    route: "{*path}",
    methods: ["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "TRACE", "CONNECT"],
    authLevel: "anonymous",
    handler: apiHandler
});

app.storageQueue("worker", {
    queueName: process.env.WORKER_QUEUE_NAME,
    connection: undefined,
    handler: workerQueueHandler,
});

app.timer("monitor", {
    schedule: "0 * * * * *",
    handler: monitor,
});
