import {
  AttributeDefinition,
  BillingMode,
  CreateTableCommand,
  KeySchemaElement,
} from "@aws-sdk/client-dynamodb";
import BladeCollection from "./BladeCollection";
import { BladeSchema, BladeSchemaKey, DynamoBladeOption } from "./BladeType";
import { BladeError, BladeErrorCode } from "./BladeError";
import { generateScalarType } from "./BladeUtility";

export default class DynamoBlade {
  public readonly option: DynamoBladeOption;

  constructor(option: DynamoBladeOption) {
    this.option = option;
  }

  async init(billingMode: BillingMode = "PAY_PER_REQUEST") {
    const keySchema: Array<KeySchemaElement> = [];
    const attributes: Array<AttributeDefinition> = [];

    if (this.option.schema.hashKey) {
      keySchema.push({
        AttributeName: this.option.schema.hashKey.field,
        KeyType: "HASH",
      });
      attributes.push({
        AttributeName: this.option.schema.hashKey.field,
        AttributeType: generateScalarType(this.option.schema.hashKey.type),
      });
    } else {
      throw new BladeError(BladeErrorCode.KeyNotSet, "No HASH field is set");
    }

    if (this.option.schema.sortKey) {
      keySchema.push({
        AttributeName: this.option.schema.sortKey.field,
        KeyType: "RANGE",
      });
      attributes.push({
        AttributeName: this.option.schema.sortKey.field,
        AttributeType: generateScalarType(this.option.schema.sortKey.type),
      });
    }

    const command = new CreateTableCommand({
      TableName: this.option.table,
      KeySchema: keySchema,
      AttributeDefinitions: attributes,
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

  collection<Schema extends BladeSchema>(
    schema: Schema,
    key: BladeSchemaKey<Schema>
  ) {
    return new BladeCollection(this.option, schema, key);
  }
}
