import {
  BillingMode,
  CreateTableCommand,
} from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  PutCommand,
  UpdateCommand,
  TransactWriteCommand,
  TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { Option, Model } from "./BladeType";
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

  open<T>(collection: T extends Model<Schema> ? T : Model<Schema>) {
    return new BladeCollection<Schema[typeof collection]>(this.option.openCollection(collection));
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
    commands: Array<PutCommand | UpdateCommand | DeleteCommand>
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
      }
    }

    if (input.TransactItems.length > 0) {
      const transaction = new TransactWriteCommand(input);
      const docClient = DynamoDBDocumentClient.from(this.option.client);

      try {
        const result = await docClient.send(transaction);
        return result.$metadata.httpStatusCode === 200;
      } catch (err) {
        console.warn(
          `Failed to transact ${input.TransactItems.length} items (${err.message})`
        );
        return false;
      }
    } else {
      return true;
    }
  }
}
