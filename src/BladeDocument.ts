import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import BladeCollection from "./BladeCollection";
import {
  BladeOption,
  BladeSchema,
  BladeType,
  BladeTypeAdd,
  BladeTypeField,
  BladeTypeUpdate,
} from "./BladeType";
import {
  getDbKey,
  getDbExtra,
  getDbValue,
  getUpdateData,
} from "./BladeUtility";
import { ProvisionedThroughputExceededException } from "@aws-sdk/client-dynamodb";
import { BladeError } from "./BladeError";

export class BladeDocument<
  Schema extends BladeSchema,
  Type extends BladeType<any>
> {
  private readonly option: BladeOption<Schema>;
  private readonly key: Array<string>;

  constructor(option: BladeOption<Schema>, key: Array<string>) {
    this.option = option;
    this.key = key;
  }

  open<T extends string & keyof BladeTypeField<Type["type"]>>(type: T) {
    return new BladeCollection<Schema, Type["type"][T]>(this.option, [
      ...this.key,
      type,
    ]);
  }

  async get(consistent?: boolean) {
    const command = new GetCommand({
      TableName: this.option.schema.table.name,
      ConsistentRead: consistent,
      Key: getDbKey(this.option.schema, this.key),
    });

    const result = await this.option.client.send(command);

    if (result.$metadata.httpStatusCode === 200) {
      return getDbValue(this.option.schema, this.key, result.Item);
    }
  }

  async add(value: BladeTypeAdd<Type["type"]>) {
    const data = {
      ...getDbValue(this.option.schema, this.key, value),
      ...getDbKey(this.option.schema, this.key),
      ...getDbExtra(this.option.schema, this.key, "ADD"),
    };

    const command = new PutCommand({
      TableName: this.option.schema.table.name,
      Item: data,
    });

    const result = await this.option.client.send(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async set(value: BladeTypeUpdate<Type["type"]>) {
    const data = getUpdateData(this.option.schema, this.key, value);

    const command = new UpdateCommand({
      TableName: this.option.schema.table.name,
      Key: getDbKey(this.option.schema, this.key),
      ...data,
    });

    const result = await this.option.client.send(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async remove() {
    const command = new DeleteCommand({
      TableName: this.option.schema.table.name,
      Key: getDbKey(this.option.schema, this.key),
    });

    const result = await this.option.client.send(command);
    return result.$metadata.httpStatusCode === 200;
  }
}
