import {
  ConditionCheck,
  CreateTableCommand,
  DynamoDBClient,
  TransactWriteItem,
} from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  PutCommand,
  UpdateCommand,
  TransactWriteCommand,
  GetCommand,
  TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";

import DynamoBladeCollection from "./DynamoBladeCollection";

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

  async transact(
    commands: Array<PutCommand | UpdateCommand | DeleteCommand | ConditionCheck>
  ) {
    const input: TransactWriteCommandInput = {
      ClientRequestToken: Date.now().toString(),
      TransactItems: [],
    };

    for (const command of commands) {
      if (command instanceof PutCommand) {
        input.TransactItems.push({
          Put: {
            TableName: command.input.TableName,
            ConditionExpression: command.input.ConditionExpression,
            ExpressionAttributeNames: command.input.ExpressionAttributeNames,
            ExpressionAttributeValues: command.input.ExpressionAttributeValues,
            Item: command.input.Item,
          },
        });
      } else if (command instanceof UpdateCommand) {
        input.TransactItems.push({
          Update: {
            TableName: command.input.TableName,
            Key: command.input.Key,
            ConditionExpression: command.input.ConditionExpression,
            ExpressionAttributeNames: command.input.ExpressionAttributeNames,
            ExpressionAttributeValues: command.input.ExpressionAttributeValues,
            UpdateExpression: command.input.UpdateExpression,
          },
        });
      } else if (command instanceof DeleteCommand) {
        input.TransactItems.push({
          Delete: {
            TableName: command.input.TableName,
            Key: command.input.Key,
            ConditionExpression: command.input.ConditionExpression,
            ExpressionAttributeNames: command.input.ExpressionAttributeNames,
            ExpressionAttributeValues: command.input.ExpressionAttributeValues,
          },
        });
      } else if (command["Key"] && command["ConditionExpression"]) {
        const { hashKey, sortKey } = this.option;

        input.TransactItems.push({
          ConditionCheck: {
            Key: {
              [hashKey]: command.Key[hashKey].S,
              [sortKey]: command.Key[sortKey].S,
            },
            TableName: command.TableName,
            ConditionExpression: command.ConditionExpression,
            ExpressionAttributeNames: command.ExpressionAttributeNames,
            ExpressionAttributeValues: command.ExpressionAttributeValues,
            ReturnValuesOnConditionCheckFailure:
              command.ReturnValuesOnConditionCheckFailure,
          },
        });
      }
    }

    if (input.TransactItems.length > 0) {
      const transaction = new TransactWriteCommand(input);

      const docClient = DynamoDBDocumentClient.from(this.option.client);

      const result = await docClient.send(transaction).catch((err) => err);
      return result.$metadata.httpStatusCode === 200;
    } else {
      return false;
    }
  }
}
