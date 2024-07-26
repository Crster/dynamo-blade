import { randomUUID } from "crypto";
import { BillingMode, CreateTableCommand } from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  PutCommand,
  UpdateCommand,
  QueryCommand,
  TransactWriteCommand,
  TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { Option, FieldType, Entity } from './BladeType';
import BladeOption from "./BladeOption";
import BladeCollection from "./BladeCollection";

export default class DynamoBlade<Schema> {
  public option: BladeOption;

  constructor(option: Option) {
    if (!option) throw new Error("Option is required");
    if (!option.tableName) throw new Error("option.tableName is required");
    if (!option.client) throw new Error("option.client is required");

    this.option = new BladeOption(option);
  }

  open<C extends keyof Entity<Schema>>(collection: C) {
    return new BladeCollection<Schema[C]>(
      this.option.openCollection(collection)
    );
  }

  key(primaryKey: string, collection?: string) {
    return this.option.getKey(primaryKey, collection);
  }

  field(type: FieldType) {
    return this.option.getFieldName(type);
  }

  async init(billingMode: BillingMode = "PAY_PER_REQUEST") {
    const { client, tableName, getFieldName } = this.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const command = new CreateTableCommand({
      TableName: tableName,
      KeySchema: [
        {
          AttributeName: getFieldName("HASH"),
          KeyType: "HASH",
        },
        {
          AttributeName: getFieldName("SORT"),
          KeyType: "RANGE",
        },
      ],
      AttributeDefinitions: [
        {
          AttributeName: getFieldName("HASH"),
          AttributeType: "S",
        },
        {
          AttributeName: getFieldName("SORT"),
          AttributeType: "S",
        },
        {
          AttributeName: getFieldName("HASH_INDEX"),
          AttributeType: "S",
        },
        {
          AttributeName: getFieldName("SORT_INDEX"),
          AttributeType: "S",
        },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: getFieldName("INDEX"),
          KeySchema: [
            {
              AttributeName: getFieldName("HASH_INDEX"),
              KeyType: "HASH",
            },
            {
              AttributeName: getFieldName("SORT_INDEX"),
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
      } else {
        console.warn(`Failed to initialize ${tableName} (${err.message})`);
      }

      return false;
    }
  }

  async transact(
    commands: Array<PutCommand | UpdateCommand | DeleteCommand | QueryCommand>,
    retry?: boolean
  ) {
    const input: TransactWriteCommandInput = {
      ClientRequestToken: randomUUID(),
      TransactItems: [],
    };

    const itemKeys = [];

    for (const command of commands) {
      if (command instanceof PutCommand) {
        if (
          command.input.Item &&
          command.input.Item[this.option.getFieldName("HASH")] &&
          command.input.Item[this.option.getFieldName("SORT")]
        ) {
          itemKeys.push({
            Type: "PUT",
            Key: `${command.input.Item[this.option.getFieldName("HASH")]}:${
              command.input.Item[this.option.getFieldName("SORT")]
            }`,
          });
        }

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
        if (command.input.Key) {
          itemKeys.push({
            Type: "UPDATE",
            Key: `${command.input.Key[this.option.getFieldName("HASH")]}:${
              command.input.Key[this.option.getFieldName("SORT")]
            }`,
          });
        }

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
        if (command.input.Key) {
          itemKeys.push({
            Type: "UPDATE",
            Key: `${command.input.Key[this.option.getFieldName("HASH")]}:${
              command.input.Key[this.option.getFieldName("SORT")]
            }`,
          });
        }

        input.TransactItems.push({
          Delete: {
            TableName: command.input.TableName,
            Key: command.input.Key,
            ConditionExpression: command.input.ConditionExpression,
            ExpressionAttributeNames: command.input.ExpressionAttributeNames,
            ExpressionAttributeValues: command.input.ExpressionAttributeValues,
          },
        });
      } else if (command instanceof QueryCommand) {
        if (command.input.ExclusiveStartKey) {
          itemKeys.push({
            Type: "QUERY",
            Key: `${
              command.input.ExclusiveStartKey[this.option.getFieldName("HASH")]
            }:${
              command.input.ExclusiveStartKey[this.option.getFieldName("SORT")]
            }`,
          });
        }

        input.TransactItems.push({
          ConditionCheck: {
            TableName: command.input.TableName,
            Key: command.input.ExclusiveStartKey,
            ConditionExpression: command.input.FilterExpression,
            ExpressionAttributeNames: command.input.ExpressionAttributeNames,
            ExpressionAttributeValues: command.input.ExpressionAttributeValues,
          },
        });
      }
    }

    if (input.TransactItems.length > 0) {
      let retryCount = retry ? 3 : 1;
      const transaction = new TransactWriteCommand(input);
      const docClient = DynamoDBDocumentClient.from(this.option.client);
      let lastestError = null;

      do {
        try {
          const result = await docClient.send(transaction);
          return result.$metadata.httpStatusCode === 200;
        } catch (err) {
          lastestError = err;

          if (err.$fault === "client") {
            retryCount--;
          } else {
            retryCount = 0;
          }
        }
      } while (retryCount > 0);

      console.warn(
        JSON.stringify({
          message: `Failed to transact ${input.TransactItems.length} items (${lastestError.message})`,
          data: itemKeys,
        }),
        null,
        2
      );
      throw lastestError;
    } else {
      return true;
    }
  }
}
