import { CreateTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import DynamoBladeCollection from "./DynamoBladeCollection";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

type Option = {
  tableName: string;
  client: DynamoDBClient;
  hashKey?: string;
  sortKey?: string;
  indexName?: string;
  separator?: string;
};

export default class DynamoBlade {
  public option: Option;

  constructor(option: Option) {
    if (!option) throw new Error("Option is required");
    if (!option.tableName) throw new Error("option.tableName is required");
    if (!option.client) throw new Error("option.client is required");

    this.option = {
      tableName: option.tableName,
      client: option.client,
      hashKey: option.hashKey || "PK",
      sortKey: option.sortKey || "SK",
      indexName: option.indexName || "GS1",
      separator: option.separator || "#",
    };
  }

  open(collection: string) {
    return new DynamoBladeCollection(this, [], collection);
  }

  async init(
    billingMode: "PROVISIONED" | "PAY_PER_REQUEST" = "PAY_PER_REQUEST"
  ) {
    const { client, tableName, hashKey, sortKey, indexName } = this.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const command = new CreateTableCommand({
      TableName: tableName,
      KeySchema: [
        {
          AttributeName: hashKey,
          KeyType: "HASH",
        },
        {
          AttributeName: sortKey,
          KeyType: "RANGE",
        },
      ],
      AttributeDefinitions: [
        {
          AttributeName: hashKey,
          AttributeType: "S",
        },
        {
          AttributeName: sortKey,
          AttributeType: "S",
        },
        {
          AttributeName: `${indexName}${hashKey}`,
          AttributeType: "S",
        },
        {
          AttributeName: `${indexName}${sortKey}`,
          AttributeType: "S",
        },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: indexName,
          KeySchema: [
            {
              AttributeName: `${indexName}${hashKey}`,
              KeyType: "HASH",
            },
            {
              AttributeName: `${indexName}${sortKey}`,
              KeyType: "RANGE",
            },
          ],
          Projection: {
            ProjectionType: "ALL",
          },
        },
      ],
      BillingMode: billingMode,
    });

    try {
      await docClient.send(command);
      return true;
    } catch (err) {
      if (err.name === "ResourceInUseException") {
        return true;
      }

      return false;
    }
  }
}
