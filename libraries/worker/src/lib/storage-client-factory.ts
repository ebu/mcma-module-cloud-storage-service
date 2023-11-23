import { S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { AwsCredentialIdentity } from "@smithy/types";

import { SecretsProvider } from "@mcma/secrets";
import { ConfigVariables, McmaException } from "@mcma/core";
import { ContainerClient } from "@azure/storage-blob";

export interface StorageClientFactoryConfig {
    configVariables?: ConfigVariables;
    secretId?: string;
    buildS3Client?: (config: S3ClientConfig) => S3Client;
    secretsProvider: SecretsProvider;
}

interface StorageClientConfig {
    aws: {
        [bucketName: string]: {
            region: string
            accessKey?: string
            secretKey?: string
            endpoint?: string
        }
    };
    azure: {
        [storageAccount: string]: {
            connectionString: string
        }
    };
}

export class StorageClientFactory {
    private storageClientConfig: StorageClientConfig;

    constructor(private config: StorageClientFactoryConfig) {
        if (!config.secretId) {
            if (!config.configVariables) {
                config.configVariables = ConfigVariables.getInstance();
            }

            config.secretId = config.configVariables.get("STORAGE_CLIENT_CONFIG_SECRET_ID");
        }

        if (!config.buildS3Client) {
            config.buildS3Client = config1 => new S3Client(config1);
        }
    }

    private async init() {
        if (!this.storageClientConfig) {
            this.storageClientConfig = await this.config.secretsProvider.getAs<StorageClientConfig>(this.config.secretId);
        }
    }

    async getS3Client(bucket: string, region?: string): Promise<S3Client> {
        await this.init();

        const bucketConfig = this.storageClientConfig.aws[bucket];
        if (!bucketConfig) {
            throw new McmaException(`Storage client config not found for S3 bucket '${bucket}'`);
        }

        console.log(`StorageClientFactory.getS3Client(${bucket}}`);
        console.log(JSON.stringify(bucketConfig, null, 2));

        const credentials: AwsCredentialIdentity = bucketConfig.accessKey && bucketConfig.secretKey ? {
            accessKeyId: bucketConfig.accessKey,
            secretAccessKey: bucketConfig.secretKey
        } : undefined;

        let endpoint = undefined;
        let forcePathStyle = undefined;

        if (bucketConfig.endpoint) {
            endpoint = bucketConfig.endpoint;
            forcePathStyle = true;
        }

        return this.config.buildS3Client({ credentials, region: region ?? bucketConfig.region, endpoint, forcePathStyle });
    }

    async getContainerClient(account: string, container: string): Promise<ContainerClient> {
        await this.init();

        const accountConfig = this.storageClientConfig.azure[account];
        if (!accountConfig) {
            throw new McmaException(`Storage client config not found for Azure storage account '${account}'`);
        }

        return new ContainerClient(accountConfig.connectionString, container);
    }
}
