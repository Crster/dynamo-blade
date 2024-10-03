import {
  AttributeDefinition,
  CreateTableCommand,
  GlobalSecondaryIndex,
  KeySchemaElement,
} from "@aws-sdk/client-dynamodb";
import { randomUUID } from "crypto";
import {
  DeleteCommand,
  PutCommand,
  UpdateCommand,
  QueryCommand,
  ScanCommand,
  TransactWriteCommand,
  TransactWriteCommandInput,
  DynamoDBDocumentClient,
} from "@aws-sdk/lib-dynamodb";
import { BladeError, BladeErrorHandler } from "./BladeError";
import { BladeTable, BladeTableOption } from "./BladeTable";
import { addToAttributeDefinition, getFieldKind } from "./BladeUtility";
export class DynamoBlade {
  public readonly client: DynamoDBDocumentClient;
  private readonly tables: Record<string, BladeTable<BladeTableOption>>;
  private readonly tablePrefix: string;
  private errorHandler: BladeErrorHandler;

  constructor(client: DynamoDBDocumentClient, name?: string) {
    this.client = client;
    this.tablePrefix = name || "";
    this.tables = {};
  }

  table<Table extends BladeTable<BladeTableOption>>(table: Table) {
    let tableName: string;

    if (table instanceof BladeTable) {
      tableName = table.name;

      if (!this.tables[tableName]) {
        table.dynamoBlade = this;
        table.namePrefix = this.tablePrefix;

        this.tables[tableName] = table;
      }
    }

    return this.tables[tableName] as Table;
  }

  private async _init(tableName: string) {
    const table = this.tables[tableName];
    if (table) {
      const keySchema: Array<KeySchemaElement> = [];
      const localIndex: Array<GlobalSecondaryIndex> = [];
      const globalIndex: Array<GlobalSecondaryIndex> = [];
      const attributes: Array<AttributeDefinition> = [];

      const hashKey = getFieldKind(table.option.keySchema, "HashKey").at(0);
      const sortKey = getFieldKind(table.option.keySchema, "SortKey").at(0);

      if (hashKey) {
        keySchema.push({
          AttributeName: hashKey.field,
          KeyType: "HASH",
        });
        addToAttributeDefinition(attributes, hashKey.field, hashKey.type);
      } else {
        this.throwError("HASH field is required", "TypeError");
      }

      if (sortKey) {
        keySchema.push({
          AttributeName: sortKey.field,
          KeyType: "RANGE",
        });
        addToAttributeDefinition(attributes, sortKey.field, sortKey.type);
      }

      for (const indexName in table.option.index) {
        const index = table.option.index[indexName];
        const indexHashKey = getFieldKind(index.option.keySchema, "HashKey").at(
          0
        );
        const indexSortKey = getFieldKind(index.option.keySchema, "SortKey").at(
          0
        );

        if (index.type === "GLOBAL") {
          const globalKeySchema: Array<KeySchemaElement> = [];
          if (indexHashKey) {
            globalKeySchema.push({
              AttributeName: indexHashKey.field,
              KeyType: "HASH",
            });
            addToAttributeDefinition(
              attributes,
              indexHashKey.field,
              indexHashKey.type
            );
          }

          if (indexSortKey) {
            globalKeySchema.push({
              AttributeName: indexSortKey.field,
              KeyType: "RANGE",
            });
            addToAttributeDefinition(
              attributes,
              indexSortKey.field,
              indexSortKey.type
            );
          }

          globalIndex.push({
            IndexName: indexName,
            KeySchema: globalKeySchema,
            Projection: index.option.projection,
            OnDemandThroughput: index.option.onDemandThroughput,
            ProvisionedThroughput:
              index.option.provisionedThroughput ??
              table.option.provisionedThroughput,
          });
        } else if (index.type === "LOCAL") {
          const localKeySchema: Array<KeySchemaElement> = [];
          if (hashKey) {
            localKeySchema.push({
              AttributeName: hashKey.field,
              KeyType: "HASH",
            });
          }

          if (indexSortKey) {
            localKeySchema.push({
              AttributeName: indexSortKey.field,
              KeyType: "RANGE",
            });
            addToAttributeDefinition(
              attributes,
              indexSortKey.field,
              indexSortKey.type
            );
          }

          localIndex.push({
            IndexName: indexName,
            KeySchema: localKeySchema,
            Projection: index.option.projection,
            OnDemandThroughput: index.option.onDemandThroughput,
            ProvisionedThroughput: index.option.provisionedThroughput,
          });
        }
      }

      const command = new CreateTableCommand({
        TableName: table.name,
        KeySchema: keySchema,
        AttributeDefinitions: attributes,
        BillingMode: table.option.billingMode,
        ProvisionedThroughput: table.option.provisionedThroughput,
        LocalSecondaryIndexes: localIndex.length ? localIndex : undefined,
        GlobalSecondaryIndexes: globalIndex.length ? globalIndex : undefined,
      });

      try {
        await this.client.send(command);
        return true;
      } catch (err) {
        if (err.name === "ResourceInUseException") {
          return true;
        }
      }
    }

    return false;
  }

  async init(): Promise<[boolean, Record<string, boolean>]> {
    let success: boolean = true;
    let results: Record<string, boolean> = {};

    for (const table in this.tables) {
      const result = await this._init(table);

      if (success) {
        success = result;
      }

      results[table] = result;
    }

    return [success, results];
  }

  async transact(
    commands: Array<
      PutCommand | UpdateCommand | DeleteCommand | QueryCommand | ScanCommand
    >,
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
        const keyValues = new Map<string, string>();
        const attributeNames = new Map<string, string>();
        const attributeValues = new Map<string, string>();

        for (const k in command.input.ExpressionAttributeNames) {
          if (command.input.FilterExpression.includes(k)) {
            attributeNames.set(k, command.input.ExpressionAttributeNames[k]);
          } else if (command.input.KeyConditionExpression.includes(k)) {
            const counter = k.match(/\d+/);

            if (counter && counter.length === 1) {
              keyValues.set(
                command.input.ExpressionAttributeNames[k],
                command.input.ExpressionAttributeValues[`:value${counter[0]}`]
              );
            }
          }
        }

        for (const k in command.input.ExpressionAttributeValues) {
          if (command.input.FilterExpression.includes(k)) {
            attributeValues.set(k, command.input.ExpressionAttributeValues[k]);
          }
        }

        input.TransactItems.push({
          ConditionCheck: {
            TableName: command.input.TableName,
            Key: Object.fromEntries(keyValues),
            ConditionExpression: command.input.FilterExpression,
            ExpressionAttributeNames: Object.fromEntries(attributeNames),
            ExpressionAttributeValues: Object.fromEntries(attributeValues),
          },
        });
      } else if (command instanceof ScanCommand) {
        const attributeNames = new Map<string, string>();
        const attributeValues = new Map<string, string>();

        for (const k in command.input.ExpressionAttributeNames) {
          if (command.input.FilterExpression.includes(k)) {
            attributeNames.set(k, command.input.ExpressionAttributeNames[k]);
          }
        }

        for (const k in command.input.ExpressionAttributeValues) {
          if (command.input.FilterExpression.includes(k)) {
            attributeValues.set(k, command.input.ExpressionAttributeValues[k]);
          }
        }

        input.TransactItems.push({
          ConditionCheck: {
            TableName: command.input.TableName,
            Key: command.input.ExclusiveStartKey,
            ConditionExpression: command.input.FilterExpression,
            ExpressionAttributeNames: Object.fromEntries(attributeNames),
            ExpressionAttributeValues: Object.fromEntries(attributeValues),
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
          const result = await this.client.send(transaction);
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

      if (lastestError) {
        this.throwError(
          `Failed to transact ${input.TransactItems.length} items (${
            lastestError.message ?? "Unknown"
          })`,
          lastestError["name"] ?? "OperationError"
        );
      }
    } else {
      return true;
    }
  }

  onError(errorHandler: BladeErrorHandler) {
    this.errorHandler = errorHandler;
  }

  throwError(message: string, reason?: string) {
    if (typeof this.errorHandler === "function") {
      this.errorHandler(new BladeError(message, reason));
    }
  }
}
