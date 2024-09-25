import {
  BladeAttribute,
  BladeItem,
  BladeAttributeSchema,
  BladeSchema,
} from "./BladeAttribute";
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { BladeFieldKind } from "./BladeField";
import { getFieldKind } from "./BladeUtility";
import { BladeCollection } from "./BladeCollection";
import { BladeView, DataFilter, KeyFilter } from "./BladeView";
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
    return new BladeView<Attribute>(this.blade).and(field, condition, value);
  }

  async get(consistent?: boolean) {
    const command = new GetCommand({
      TableName: this.blade.getTableName(),
      ConsistentRead: consistent,
      Key: this.blade.getKeyValue(),
    });

    const result = await this.blade.execute(command);

    if (result.$metadata.httpStatusCode === 200) {
      return this.blade.buildItem<BladeItem<Attribute>>(result["Item"]);
    }
  }

  async add(value: BladeAttributeForAdd<Attribute>, overwrite?: boolean) {
    const command = new PutCommand({
      TableName: this.blade.getTableName(),
      Item: this.blade.getNewItem(value),
      ConditionExpression: this.blade.buildAddCondition(overwrite),
    });

    const result = await this.blade.execute(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async set(
    value: BladeAttributeForUpdate<Attribute>,
    condition?: [
      string & keyof BladeItem<Attribute>,
      KeyFilter | DataFilter,
      any
    ]
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
