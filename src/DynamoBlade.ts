import {
  AttributeDefinition,
  BillingMode,
  CreateTableCommand,
  GlobalSecondaryIndex,
  KeySchemaElement,
} from "@aws-sdk/client-dynamodb";
import { BladeView } from "./BladeView";
import { BladeError } from "./BladeError";
import { BladeCollection } from "./BladeCollection";
import { addToAttributeDefinition } from "./BladeUtility";
import { BladeOption, BladeSchema, BladeTypeField } from "./BladeType";

export default class DynamoBlade<Schema extends BladeSchema> {
  public readonly option: BladeOption<Schema>;

  constructor(option: BladeOption<Schema>) {
    this.option = option;
  }

  async init(billingMode: BillingMode = "PAY_PER_REQUEST") {
    const keySchema: Array<KeySchemaElement> = [];
    const localIndex: Array<GlobalSecondaryIndex> = [];
    const globalIndex: Array<GlobalSecondaryIndex> = [];
    const attributes: Array<AttributeDefinition> = [];

    if (this.option.schema.table.hashKey) {
      keySchema.push({
        AttributeName: this.option.schema.table.hashKey,
        KeyType: "HASH",
      });
      addToAttributeDefinition(
        attributes,
        this.option.schema.table.hashKey,
        "S"
      );
    } else {
      throw new BladeError("NO_HASHKEY", "No HASH field is set");
    }

    if (this.option.schema.table.sortKey) {
      keySchema.push({
        AttributeName: this.option.schema.table.sortKey,
        KeyType: "RANGE",
      });
      addToAttributeDefinition(
        attributes,
        this.option.schema.table.sortKey,
        "S"
      );
    }

    for (const indexName in this.option.schema.index) {
      const index = this.option.schema.index[indexName];

      if (index.type === "GLOBAL") {
        const globalKeySchema: Array<KeySchemaElement> = [];
        if (index.hashKey) {
          globalKeySchema.push({
            AttributeName: index.hashKey[0],
            KeyType: "HASH",
          });
          addToAttributeDefinition(
            attributes,
            index.hashKey[0],
            index.hashKey[1]
          );
        }

        if (index.sortKey) {
          globalKeySchema.push({
            AttributeName: index.sortKey[0],
            KeyType: "RANGE",
          });
          addToAttributeDefinition(
            attributes,
            index.sortKey[0],
            index.sortKey[1]
          );
        }

        globalIndex.push({
          IndexName: indexName,
          Projection: index.projection ?? { ProjectionType: "ALL" },
          ProvisionedThroughput:
            billingMode === "PROVISIONED"
              ? { ReadCapacityUnits: 20, WriteCapacityUnits: 10 }
              : undefined,
          OnDemandThroughput: index.throughput,
          KeySchema: globalKeySchema,
        });
      } else if (index.type === "LOCAL") {
        const localKeySchema: Array<KeySchemaElement> = [];
        if (this.option.schema.table.hashKey) {
          localKeySchema.push({
            AttributeName: this.option.schema.table.hashKey,
            KeyType: "HASH",
          });
        }

        if (index.sortKey) {
          localKeySchema.push({
            AttributeName: index.sortKey[0],
            KeyType: "RANGE",
          });
          addToAttributeDefinition(
            attributes,
            index.sortKey[0],
            index.sortKey[1]
          );
        }

        localIndex.push({
          IndexName: indexName,
          Projection: index.projection ?? { ProjectionType: "ALL" },
          OnDemandThroughput: index.throughput,
          KeySchema: localKeySchema,
        });
      }
    }

    const command = new CreateTableCommand({
      TableName: this.option.schema.table.name,
      KeySchema: keySchema,
      AttributeDefinitions: attributes,
      ProvisionedThroughput:
        billingMode === "PROVISIONED"
          ? { ReadCapacityUnits: 20, WriteCapacityUnits: 10 }
          : undefined,
      LocalSecondaryIndexes: localIndex.length ? localIndex : undefined,
      GlobalSecondaryIndexes: globalIndex.length ? globalIndex : undefined,
      BillingMode: billingMode,
    });

    try {
      await this.option.client.send(command);
      return true;
    } catch (err) {
      if (err.name === "ResourceInUseException") {
        return true;
      }
    }

    return false;
  }

  open<T extends string & keyof BladeTypeField<Schema["type"]>>(type: T) {
    return new BladeCollection<Schema, Schema["type"][T]>(this.option, [type]);
  }

  query<T extends string & keyof Schema["index"]>(index: T) {
    return new BladeView(this.option, [], "QUERY", index);
  }

  scan<T extends string & keyof Schema["index"]>(index: T) {
    return new BladeView(this.option, [], "SCAN", index);
  }
}
