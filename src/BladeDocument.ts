import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  BladeAttribute,
  BladeItem,
  BladeAttributeSchema,
  BladeSchema,
} from "./BladeAttribute";
import { BladeFieldKind } from "./BladeField";
import { getFieldKind } from "./BladeUtility";
import { BladeCollection } from "./BladeCollection";
import { ArrayResult, BladeView, DataFilter, KeyFilter } from "./BladeView";
import { Blade, BladeAttributeForAdd, BladeAttributeForUpdate } from "./Blade";

export class BladeDocument<
  Attribute extends BladeAttribute<BladeAttributeSchema>
> {
  private readonly blade: Blade<any>;

  constructor(blade: Blade<any>) {
    this.blade = blade;
  }

  getKey(kind: BladeFieldKind) {
    const keyValue = this.blade.getKeyValue();
    const hashKey = getFieldKind(this.blade.table.option.keySchema, kind).at(0);

    if (keyValue && hashKey) {
      return keyValue[hashKey.field];
    }
  }

  open<T extends string & keyof BladeSchema<Attribute>>(type: T) {
    return new BladeCollection<BladeSchema<Attribute>[T]>(
      this.blade.open(type)
    );
  }

  where(
    field: string & keyof BladeItem<Attribute>,
    condition: KeyFilter | DataFilter,
    value: any
  ) {
    return new BladeView<BladeItem<Attribute>, ArrayResult<Attribute>>(
      this.blade,
      {
        count: 0,
        data: [],
      }
    ).and(field, condition, value);
  }

  async get(consistent?: boolean) {
    try {
      const command = new GetCommand({
        TableName: this.blade.getTableName(),
        ConsistentRead: consistent,
        Key: this.blade.getKeyValue(),
      });

      const result = await this.blade.execute(command);

      if (result.$metadata.httpStatusCode === 200) {
        return this.blade.buildItem<BladeItem<Attribute>>(result["Item"]);
      }
    } catch (err) {
      this.blade.throwError(
        `Failed to get ${this.blade.getKeyString()} (${
          err.message ?? "Unknown"
        })`,
        err["name"] ?? "OperationError"
      );
    }
  }

  addLater(value: BladeAttributeForAdd<Attribute>, overwrite: boolean = true) {
    const command = new PutCommand({
      TableName: this.blade.getTableName(),
      Item: this.blade.getNewItem(value),
      ConditionExpression: this.blade.buildAddCondition(overwrite),
    });

    return command;
  }

  async add(value: BladeAttributeForAdd<Attribute>, overwrite: boolean = true) {
    try {
      const command = this.addLater(value, overwrite);
      const result = await this.blade.execute(command);
      return result.$metadata.httpStatusCode === 200;
    } catch (err) {
      this.blade.throwError(
        `Failed to add ${this.blade.getKeyString()} (${
          err.message ?? "Unknown"
        })`,
        err["name"] ?? "OperationError"
      );
    }

    return false;
  }

  setLater(
    value: BladeAttributeForUpdate<Attribute>,
    condition?: BladeView<any, any>
  ) {
    const update = this.blade.getUpdateItem(value, condition);

    const command = new UpdateCommand({
      TableName: this.blade.getTableName(),
      Key: this.blade.getKeyValue(),
      ...update,
    });

    return command;
  }

  async set(
    value: BladeAttributeForUpdate<Attribute>,
    condition?: BladeView<any, any>
  ) {
    try {
      const command = this.setLater(value, condition);

      const result = await this.blade.execute(command);
      return result.$metadata.httpStatusCode === 200;
    } catch (err) {
      this.blade.throwError(
        `Failed to set ${this.blade.getKeyString()} (${
          err.message ?? "Unknown"
        })`,
        err["name"] ?? "OperationError"
      );
    }

    return false;
  }

  removeLater() {
    const command = new DeleteCommand({
      TableName: this.blade.getTableName(),
      Key: this.blade.getKeyValue(),
    });

    return command;
  }

  async remove() {
    try {
      const command = this.removeLater();

      const result = await this.blade.execute(command);
      return result.$metadata.httpStatusCode === 200;
    } catch (err) {
      this.blade.throwError(
        `Failed to remove ${this.blade.getKeyString()} (${
          err.message ?? "Unknown"
        })`,
        err["name"] ?? "OperationError"
      );
    }

    return false;
  }
}
