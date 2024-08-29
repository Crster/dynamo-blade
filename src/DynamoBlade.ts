import { randomUUID } from "crypto";
import {
  DeleteCommand,
  PutCommand,
  UpdateCommand,
  QueryCommand,
  TransactWriteCommand,
  TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";
import {
  AttributeDefinition,
  BillingMode,
  CreateTableCommand,
  KeySchemaElement,
} from "@aws-sdk/client-dynamodb";

import { Option, CollectionName } from "./BladeType";
import BladeCollection from "./BladeCollection";

export default class DynamoBlade<Opt extends Option> {
  public option: Opt;

  constructor(option: Opt) {
    if (!option) throw new Error("Option is required");
    if (!option.tableName) throw new Error("option.tableName is required");
    if (!option.client) throw new Error("option.client is required");
    if (!option.primaryKey) throw new Error("option.index is required");
    if (!option.schema) throw new Error("option.schema is required");

    this.option = option;
  }

  open<Collection extends string & CollectionName<Opt>>(
    collection: Collection
  ) {
    return new BladeCollection<Opt, Collection>(this.option, collection);
  }

  async init(billingMode: BillingMode = "PAY_PER_REQUEST") {
    const { client, tableName, primaryKey } = this.option;

    const keySchema: Array<KeySchemaElement> = [];
    const attributeDefinitions: Array<AttributeDefinition> = [];

    if (primaryKey.hashKey) {
      keySchema.push({
        AttributeName: primaryKey.hashKey[0],
        KeyType: "HASH",
      });

      if (primaryKey.hashKey[1] instanceof Number) {
        attributeDefinitions.push({
          AttributeName: primaryKey.hashKey[0],
          AttributeType: "N",
        });
      }
      if (primaryKey.hashKey[1] instanceof Buffer) {
        attributeDefinitions.push({
          AttributeName: primaryKey.hashKey[0],
          AttributeType: "B",
        });
      } else {
        attributeDefinitions.push({
          AttributeName: primaryKey.hashKey[0],
          AttributeType: "S",
        });
      }
    }

    if (primaryKey.sortKey) {
      keySchema.push({
        AttributeName: primaryKey.sortKey[0],
        KeyType: "RANGE",
      });

      if (primaryKey.sortKey[1] instanceof Number) {
        attributeDefinitions.push({
          AttributeName: primaryKey.sortKey[0],
          AttributeType: "N",
        });
      }
      if (primaryKey.sortKey[1] instanceof Buffer) {
        attributeDefinitions.push({
          AttributeName: primaryKey.sortKey[0],
          AttributeType: "B",
        });
      } else {
        attributeDefinitions.push({
          AttributeName: primaryKey.sortKey[0],
          AttributeType: "S",
        });
      }
    }

    const command = new CreateTableCommand({
      TableName: tableName,
      KeySchema: keySchema,
      AttributeDefinitions: attributeDefinitions,
      BillingMode: billingMode,
    });

    try {
      await client.send(command);
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
      } else if (command instanceof QueryCommand) {
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
      let lastestError = null;

      do {
        try {
          const result = await this.option.client.send(transaction);
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

      throw lastestError;
    } else {
      return true;
    }
  }
}
