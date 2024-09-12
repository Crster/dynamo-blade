import {
  BladeSchema,
  DynamoBladeOption,
  BladeSchemaKey,
  RequiredBladeItem,
  BladeItem,
} from "./BladeType";
import BladeView from "./BladeView";
import BladeDocument from "./BladeDocument";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import { generateItem, generateKey, generateTimestamp } from "./BladeUtility";

export default class BladeCollection<
  Option extends DynamoBladeOption,
  Schema extends BladeSchema
> {
  private readonly option: Option;
  private readonly schema: Schema;
  private readonly key: BladeSchemaKey<Schema>;

  constructor(option: Option, schema: Schema, key: BladeSchemaKey<Schema>) {
    this.option = option;
    this.schema = schema;
    this.key = key;
  }

  is(keyValue: RequiredBladeItem<Schema>) {
    return new BladeDocument(this.option, this.schema, this.key, keyValue);
  }

  async add(value: BladeItem<Schema>) {
    const item = {
      ...generateItem<Schema>(this.schema, value, "ADD"),
      ...generateKey(this.option, this.key, value),
      ...generateTimestamp(this.option, "ADD"),
    };

    const command = new PutCommand({
      TableName: this.option.table,
      Item: item,
    });

    const result = await this.option.client.send(command);
    if (result.$metadata.httpStatusCode === 200) {
      return generateItem<Schema>(this.schema, item, "GET");
    }
  }

  query(index?: string & keyof Option["schema"]["index"]) {
    return new BladeView(this.option, this.schema, this.key, "QUERY", index);
  }

  queryDesc(index?: string & keyof Option["schema"]["index"]) {
    return new BladeView(this.option, this.schema, this.key, "QUERYDESC", index);
  }

  scan(index?: string & keyof Option["schema"]["index"]) {
    return new BladeView(this.option, this.schema, this.key, "SCAN", index);
  }
}
