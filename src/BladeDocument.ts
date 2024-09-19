import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  BladeType,
  BladeTypeField,
  BladeTypeUpdate,
  BladeItem,
  BladeTypeAdd,
} from "./BladeType";
import { BladeCollection } from "./BladeCollection";
import { BladeKeySchema } from "./BladeKeySchema";
import { ValueFilter, DataFilter } from "./BladeType";

export class BladeDocument<Type extends BladeType<any>> {
  private readonly blade: BladeKeySchema<any>;

  constructor(blade: BladeKeySchema<any>) {
    this.blade = blade;
  }

  open<T extends string & keyof BladeTypeField<Type["type"]>>(type: T) {
    return new BladeCollection<Type["type"][T]>(this.blade.open(type));
  }

  async get(consistent?: boolean) {
    const command = new GetCommand({
      TableName: this.blade.getTableName(),
      ConsistentRead: consistent,
      Key: this.blade.getKeyValue(),
    });

    const result = await this.blade.execute(command);

    if (result.$metadata.httpStatusCode === 200) {
      return this.blade.buildItem<BladeItem<Type["type"]>>(result["Item"]);
    }
  }

  async add(value: BladeTypeAdd<Type["type"]>, overwrite?: boolean) {
    const command = new PutCommand({
      TableName: this.blade.getTableName(),
      Item: this.blade.getNewItem(value),
      ConditionExpression: this.blade.buildAddCondition(overwrite),
    });

    const result = await this.blade.execute(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async set(
    value: BladeTypeUpdate<Type["type"]>,
    condition?: [string & keyof BladeItem<Type["type"]>, ValueFilter | DataFilter, any]
  ) {
    const update = this.blade.getUpdateItem(value, condition);

    const command = new UpdateCommand({
      TableName: this.blade.getTableName(),
      Key: this.blade.getKeyValue(),
      ...update,
    });

    const result = await this.blade.execute(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async remove() {
    const command = new DeleteCommand({
      TableName: this.blade.getTableName(),
      Key: this.blade.getKeyValue(),
    });

    const result = await this.blade.execute(command);
    return result.$metadata.httpStatusCode === 200;
  }
}
