import {
  AttributeDefinition,
  CreateTableCommand,
  GlobalSecondaryIndex,
  KeySchemaElement,
} from "@aws-sdk/client-dynamodb";
import { BladeError } from "./BladeError";
import { BladeTable, BladeTableOption } from "./BladeTable";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { addToAttributeDefinition, getFieldKind } from "./BladeUtility";

export class DynamoBlade {
  public readonly client: DynamoDBDocumentClient;
  private readonly tables: Record<string, BladeTable<BladeTableOption>>;
  private readonly tablePrefix: string;

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
        table.client = this.client;
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
        throw new BladeError("NO_HASHKEY", "No HASH field is set");
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
}
