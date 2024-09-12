import {
  DeleteCommand,
  GetCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  BladeSchema,
  BladeSchemaKey,
  DynamoBladeOption,
  RequiredBladeItem,
  UpdateBladeItem,
} from "./BladeType";
import {
  generateItem,
  generateKey,
  generateTimestamp,
  generateUpdate,
} from "./BladeUtility";

export default class BladeDocument<
  Option extends DynamoBladeOption,
  Schema extends BladeSchema
> {
  private readonly option: Option;
  private readonly schema: Schema;
  private readonly key: BladeSchemaKey<Schema>;
  private readonly keyValue: RequiredBladeItem<Schema>;

  constructor(
    option: Option,
    schema: Schema,
    key: BladeSchemaKey<Schema>,
    keyValue: RequiredBladeItem<Schema>
  ) {
    this.option = option;
    this.schema = schema;
    this.key = key;
    this.keyValue = keyValue;
  }

  async get(consistent?: boolean) {
    const command = new GetCommand({
      TableName: this.option.table,
      Key: generateKey(this.option, this.key, this.keyValue),
      ConsistentRead: consistent,
    });

    const result = await this.option.client.send(command);
    return generateItem<Schema>(this.schema, result?.Item, "GET");
  }

  async set(value: UpdateBladeItem<Schema>) {
    const data = generateUpdate(
      this.schema,
      value,
      generateTimestamp(this.option, "SET")
    );

    const command = new UpdateCommand({
      TableName: this.option.table,
      Key: generateKey(this.option, this.key, this.keyValue),
      ...data,
    });

    const result = await this.option.client.send(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async remove() {
    const command = new DeleteCommand({
      TableName: this.option.table,
      Key: generateKey(this.option, this.key, this.keyValue),
    });

    const result = await this.option.client.send(command);
    return result.$metadata.httpStatusCode === 200;
  }
}
