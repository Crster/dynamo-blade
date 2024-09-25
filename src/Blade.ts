import { BladeError } from "./BladeError";
import {
  BladeAttribute,
  BladeAttributeSchema,
  BladeItem,
  PrimaryKeyField,
} from "./BladeAttribute";
import { getCondition, getFieldKind } from "./BladeUtility";
import { BladeTable } from "./BladeTable";
import { DataFilter, KeyFilter } from "./BladeView";
import { EventHandler } from "./BladeAttribute";
import { BladeFieldKind } from "./BladeField";

interface KeyAttribute {
  dataType: string;
  primaryKey: string;
  value?: any;
  condition?: KeyFilter;
}

export type BladeAttributeForAdd<
  Type extends BladeAttribute<BladeAttributeSchema>
> = Omit<BladeItem<Type>, keyof PrimaryKeyField<Type>>;

export type BladeAttributeForUpdate<
  Type extends BladeAttribute<BladeAttributeSchema>
> =
  | Partial<BladeAttributeForAdd<Type>>
  | {
      $add: Partial<
        Record<
          keyof BladeAttributeForAdd<Type>,
          number | Set<string> | Set<number>
        >
      >;
    }
  | { $set: Partial<BladeAttributeForAdd<Type>> }
  | { $remove: Partial<Record<keyof BladeAttributeForAdd<Type>, boolean>> }
  | {
      $delete: Partial<
        Record<keyof BladeAttributeForAdd<Type>, Set<string> | Set<number>>
      >;
    };

export class Blade<Option extends BladeTable<any>> {
  public readonly table: Option;

  private index: string;
  private scanIndexForward: boolean;
  private currentSchema: BladeAttribute<BladeAttributeSchema>;
  private dataType: string;
  private keys: Array<KeyAttribute>;

  constructor(table: Option) {
    this.table = table;
    this.scanIndexForward = true;
    this.keys = [];
  }

  private _clone() {
    const clone = new Blade(this.table);
    clone.index = this.index;
    clone.scanIndexForward = this.scanIndexForward;
    clone.currentSchema = this.currentSchema;
    clone.dataType = this.dataType;
    clone.keys = this.keys;

    return clone;
  }

  open(type: string) {
    const clone = this._clone();

    if (clone.currentSchema) {
      clone.currentSchema = clone.currentSchema.schema[
        type
      ] as BladeAttribute<BladeAttributeSchema>;
    } else {
      clone.currentSchema = clone.table.option.attribute[type];
    }

    if (!clone.currentSchema) {
      throw new BladeError("TYPE_UNKNOWN", "Type not found");
    }

    const primaryKeyField = getFieldKind(
      clone.currentSchema.schema,
      "PrimaryKey"
    ).at(0);

    if (!primaryKeyField) {
      throw new BladeError(
        "PRIMARY_KEY_UNKNOWN",
        "No primary key for the type " + type
      );
    }

    if (clone.dataType) {
      clone.dataType = clone.dataType + ":" + type;
    } else {
      clone.dataType = type;
    }

    clone.keys.push({
      dataType: type,
      primaryKey: primaryKeyField.field,
    });

    return clone;
  }

  setIndex(index: string) {
    this.index = index;
    return this;
  }

  whereIndexKey(
    index: string,
    field: string,
    condition: KeyFilter,
    value: any
  ) {
    const currentIndex = this.table.option.index[index];
    if (!currentIndex) throw new BladeError("INDEX_UNKNOWN", "Index not found");

    const hashKey = getFieldKind(currentIndex.option.keySchema, "HashKey").at(
      0
    );
    const sortKey = getFieldKind(currentIndex.option.keySchema, "SortKey").at(
      0
    );

    let tmpValue: any;
    if (Array.isArray(value)) {
      tmpValue = [];
      for (const val of value) {
        if (val instanceof Date) {
          tmpValue.push(val.toISOString());
        } else {
          tmpValue.push(val);
        }
      }
    } else if (value instanceof Date) {
      tmpValue = value.toISOString();
    } else {
      tmpValue = value;
    }

    this.index = index;
    if (hashKey?.field === field) {
      if (this.scanIndexForward) {
        this.scanIndexForward = ![">", "<", ">=", "<=", "BETWEEN"].includes(
          condition
        );
      }

      this.keys.push({
        dataType: "HASHKEY",
        primaryKey: hashKey.field,
        condition: condition,
        value: tmpValue,
      });

      return this;
    }

    if (sortKey?.field === field) {
      if (this.scanIndexForward) {
        this.scanIndexForward = ![">", "<", ">=", "<=", "BETWEEN"].includes(
          condition
        );
      }

      this.keys.push({
        dataType: "SORTKEY",
        primaryKey: sortKey.field,
        condition: condition,
        value: tmpValue,
      });

      return this;
    }

    return this;
  }

  whereKey(condition: KeyFilter, value: any) {
    const key = this.keys.at(-1);
    if (!key) throw new BladeError("KEY_MISSING", "No last key in the list");

    if (this.scanIndexForward) {
      this.scanIndexForward = ![">", "<", ">=", "<=", "BETWEEN"].includes(
        condition
      );
    }

    key.condition = condition;

    if (value instanceof Date) {
      key.value = value.toISOString();
    } else {
      key.value = value;
    }

    return this;
  }

  hasKey() {
    return this.keys.length > 0;
  }

  getIndex() {
    return this.index;
  }

  getScanIndexForward() {
    return this.scanIndexForward;
  }

  getTableName() {
    if (this.table.namePrefix) {
      return this.table.namePrefix + "_" + this.table.name;
    } else {
      return this.table.name;
    }
  }

  getKeyField(kind: BladeFieldKind) {
    return getFieldKind(this.table.option.keySchema, kind);
  }

  getKey() {
    return this.keys;
  }

  getKeyValue() {
    const ret = {};

    const hashKey = getFieldKind(this.table.option.keySchema, "HashKey").at(0);
    const sortKey = getFieldKind(this.table.option.keySchema, "SortKey").at(0);

    if (hashKey && sortKey) {
      const hashKeyRange = [0, this.keys.length - 1];
      const sortKeyRange = [this.keys.length - 1, this.keys.length];

      if (this.keys.length <= 1) {
        hashKeyRange[1] = this.keys.length;
        sortKeyRange[0] = 0;
        sortKeyRange[1] = this.keys.length;
      }

      for (let xx = hashKeyRange[0]; xx < hashKeyRange[1]; xx++) {
        const key = this.keys[xx];

        if (ret[hashKey.field]) {
          ret[hashKey.field] += `${key.dataType}#${key.value}:`;
        } else {
          ret[hashKey.field] = `${key.dataType}#${key.value}:`;
        }
      }

      ret[hashKey.field] = ret[hashKey.field].slice(
        0,
        ret[hashKey.field].length - 1
      );

      for (let xx = sortKeyRange[0]; xx < sortKeyRange[1]; xx++) {
        const key = this.keys[xx];

        if (ret[sortKey.field]) {
          ret[sortKey.field] += `${key.dataType}#${key.value}:`;
        } else {
          ret[sortKey.field] = `${key.dataType}#${key.value}:`;
        }
      }

      ret[sortKey.field] = ret[sortKey.field].slice(
        0,
        ret[sortKey.field].length - 1
      );
    } else if (hashKey) {
      for (let xx = 0; xx < this.keys.length; xx++) {
        const key = this.keys[xx];

        if (ret[hashKey.field]) {
          ret[hashKey.field] += `${key.dataType}#${key.value}:`;
        } else {
          ret[hashKey.field] = `${key.dataType}#${key.value}:`;
        }
      }

      ret[hashKey.field] = ret[hashKey.field].slice(
        0,
        ret[hashKey.field].length - 1
      );
    }

    return ret;
  }

  getValue(dataType: string, value: any) {
    if (Array.isArray(value)) {
      const ret = [];
      for (const val of value) {
        if (this.index) {
          ret.push(val);
        } else {
          ret.push(`${dataType}#${val}`);
        }
      }
      return ret;
    } else {
      if (this.index) {
        return value;
      } else {
        return `${dataType}#${value}`;
      }
    }
  }

  getKeyCondition() {
    const ret: Array<KeyAttribute> = [];
    const keyValue = { hashKey: [], sortKey: [] };

    let hashKey = getFieldKind(this.table.option.keySchema, "HashKey").at(0);
    let sortKey = getFieldKind(this.table.option.keySchema, "SortKey").at(0);

    if (this.index) {
      const currentIndex = this.table.option.index[this.index];
      hashKey = getFieldKind(currentIndex.option.keySchema, "HashKey").at(0);
      sortKey = getFieldKind(currentIndex.option.keySchema, "SortKey").at(0);
    }

    if (hashKey && sortKey) {
      const hashKeyRange = [0, this.keys.length - 1];
      const sortKeyRange = [this.keys.length - 1, this.keys.length];

      if (this.keys.length <= 1) {
        hashKeyRange[1] = this.keys.length;
        sortKeyRange[0] = 0;

        if (this.index) {
          sortKeyRange[1] = this.keys.length === 1 ? 0 : this.keys.length;
        } else {
          sortKeyRange[1] = this.keys.length;
        }
      }

      let condition: KeyFilter;
      for (let xx = hashKeyRange[0]; xx < hashKeyRange[1]; xx++) {
        const key = this.keys[xx];
        condition = key.condition;
        keyValue.hashKey.push(this.getValue(key.dataType, key.value));
      }

      ret.push({
        dataType: "HASHKEY",
        primaryKey: hashKey.field,
        condition: condition,
        value:
          keyValue.hashKey.length === 1
            ? keyValue.hashKey[0]
            : keyValue.hashKey.join(":"),
      });

      if (condition === "=") {
        for (let xx = sortKeyRange[0]; xx < sortKeyRange[1]; xx++) {
          const key = this.keys[xx];
          condition = key.condition;
          keyValue.sortKey.push(this.getValue(key.dataType, key.value));
        }

        if (keyValue.sortKey.length) {
          ret.push({
            dataType: "SORTKEY",
            primaryKey: sortKey.field,
            condition: condition,
            value:
              keyValue.sortKey.length === 1
                ? keyValue.sortKey[0]
                : keyValue.sortKey.join(":"),
          });
        }
      }
    } else if (hashKey) {
      let condition: KeyFilter;
      for (let xx = 0; xx < this.keys.length; xx++) {
        const key = this.keys[xx];
        condition = key.condition;
        keyValue.hashKey.push(this.getValue(key.dataType, key.value));
      }

      ret.push({
        dataType: "HASHKEY",
        primaryKey: hashKey.field,
        condition: condition,
        value:
          keyValue.hashKey.length === 1
            ? keyValue.hashKey[0]
            : keyValue.hashKey.join(":"),
      });
    }

    return ret;
  }

  getItemValue(value: Record<string, any>, field: string) {
    const schema = this.currentSchema.schema[field];
    let fieldValue: any;

    fieldValue = value[field];

    if (schema["kind"] === "OnCreate" && fieldValue === undefined) {
      fieldValue = schema["type"](value);
    }

    if (fieldValue instanceof Date) {
      return fieldValue.toISOString();
    }

    return fieldValue;
  }

  getNewItem(value: Record<string, any>) {
    const ret = {};

    const typeKey = getFieldKind(this.table.option.keySchema, "TypeKey").at(0);
    const onCreate = getFieldKind(this.table.option.keySchema, "OnCreate");
    const onModify = getFieldKind(this.table.option.keySchema, "OnModify");

    const dbKey = this.getKeyValue();
    for (const field in dbKey) {
      ret[field] = dbKey[field];
    }

    for (const key of this.keys) {
      ret[key.primaryKey] = key.value;
    }

    if (typeKey) {
      ret[typeKey.field] = this.dataType;
    }

    const tmpValue = { ...ret, ...value };

    if (onCreate) {
      for (const autoField of onCreate) {
        const handler = autoField.bladeField.type as EventHandler;
        if (typeof handler === "function") {
          ret[autoField.field] = handler(tmpValue);
        }
      }
    }

    if (onModify) {
      for (const autoField of onModify) {
        const handler = autoField.bladeField.type as EventHandler;
        if (typeof handler === "function") {
          ret[autoField.field] = handler(tmpValue);
        }
      }
    }

    for (const field in this.currentSchema.schema) {
      const fieldValue = this.getItemValue(tmpValue, field);

      if (fieldValue !== undefined) {
        ret[field] = fieldValue;
      }
    }

    return ret;
  }

  getUpdateItem(
    value: BladeAttributeForUpdate<BladeAttribute<any>>,
    condition?: [string, KeyFilter | DataFilter, any]
  ) {
    const typeKey = getFieldKind(this.table.option.keySchema, "TypeKey").at(0);
    const onModify = getFieldKind(this.table.option.keySchema, "OnModify");

    const updates = new Map<string, { type: string; value: any }>();

    for (const key in value) {
      if (
        ["$set", "$add", "$remove", "$delete"].includes(key.toLocaleLowerCase())
      ) {
        for (const subKey in value[key]) {
          updates.set(subKey, {
            value: this.getItemValue(value[key], subKey),
            type: key,
          });
        }
      } else {
        updates.set(key, {
          value: this.getItemValue(value, key),
          type: "$set",
        });
      }
    }

    // Load Extra Data
    for (const key of this.keys) {
      updates.set(key.primaryKey, {
        value: key.value,
        type: "$set",
      });
    }

    if (typeKey) {
      updates.set(typeKey.field, {
        value: this.dataType,
        type: "$set",
      });
    }

    const tmpValue = {};
    for (const [k, v] of updates) {
      tmpValue[k] = v.value;
    }

    if (onModify) {
      for (const autoField of onModify) {
        const handler = autoField.bladeField.type as EventHandler;
        if (typeof handler === "function") {
          updates.set(autoField.field, {
            value: handler(tmpValue),
            type: "$set",
          });
        }
      }
    }

    for (const field in this.currentSchema.schema) {
      const schema = this.currentSchema.schema[field];
      if (typeof schema === "function" && schema.name === field) {
        const fieldValue = this.getItemValue(tmpValue, field);
        if (fieldValue !== undefined) {
          updates.set(field, {
            value: fieldValue,
            type: "$set",
          });
        }
      }
    }

    const updateExpression: Array<string> = [];
    const attributeName = new Map<string, string>();
    const attributeValues = new Map<string, any>();
    const field = { counter: 0, set: [], add: [], remove: [], delete: [] };
    for (const [k, v] of updates) {
      switch (v.type) {
        case "$set":
          field.set.push(`#prop${field.counter}=:value${field.counter}`);
          attributeName.set(`#prop${field.counter}`, k);
          attributeValues.set(`:value${field.counter}`, v.value);
          field.counter++;
          break;
        case "$add":
          field.add.push(`#prop${field.counter} :value${field.counter}`);
          attributeName.set(`#prop${field.counter}`, k);
          attributeValues.set(`:value${field.counter}`, v.value);
          field.counter++;
          break;
        case "$remove":
          field.remove.push(`#prop${field.counter}`);
          attributeName.set(`#prop${field.counter}`, k);
          field.counter++;
          break;
        case "$delete":
          field.delete.push(`#prop${field.counter} :value${field.counter}`);
          attributeName.set(`#prop${field.counter}`, k);
          attributeValues.set(`:value${field.counter}`, v.value);
          field.counter++;
          break;
      }
    }

    // Populate Update Expression
    if (field.set.length > 0) {
      updateExpression.push(`SET ${field.set.join(",")}`);
    }
    if (field.add.length > 0) {
      updateExpression.push(`ADD ${field.add.join(",")}`);
    }
    if (field.remove.length > 0) {
      updateExpression.push(`REMOVE ${field.remove.join(",")}`);
    }
    if (field.delete.length > 0) {
      updateExpression.push(`DELETE ${field.delete.join(",")}`);
    }

    const ret = {};
    if (condition) {
      const conditionExpression = getCondition(
        condition[0],
        condition[1],
        condition[2],
        field.counter
      );
      if (conditionExpression && conditionExpression.filter.length) {
        ret["ConditionExpression"] = conditionExpression.filter[0];

        for (const k in conditionExpression.field) {
          attributeName.set(k, conditionExpression.field[k]);
        }

        for (const k in conditionExpression.values) {
          attributeValues.set(k, conditionExpression.values[k]);
        }
      }
    }

    ret["UpdateExpression"] = updateExpression.join("\n");
    ret["ExpressionAttributeNames"] = Object.fromEntries(attributeName);
    ret["ExpressionAttributeValues"] = Object.fromEntries(attributeValues);

    return ret;
  }

  execute(command: any) {
    return this.table.client.send(command);
  }

  buildItem<Type>(
    value: Record<string, any>,
    currentSchema?: BladeAttribute<BladeAttributeSchema>
  ) {
    if (value) {
      const ret = {};
      const tmpSchema = currentSchema?.schema ?? this.currentSchema.schema;

      for (const field in tmpSchema) {
        const schema = tmpSchema[field];

        let fieldValue = value[field];

        if (
          fieldValue === undefined &&
          schema["type"] &&
          typeof schema["type"] === "function" &&
          schema["type"].name === ""
        ) {
          fieldValue = schema["type"](value);
        }

        if (fieldValue !== undefined) {
          if (schema instanceof Date) {
            ret[field] = new Date(fieldValue);
          } else if (schema["type"] instanceof Date) {
            ret[field] = new Date(fieldValue);
          } else {
            ret[field] = fieldValue;
          }
        }
      }

      return ret as Type;
    }
  }

  buildAddCondition(overwrite?: boolean) {
    if (!overwrite) {
      const hashKey = getFieldKind(this.table.option.keySchema, "HashKey").at(
        0
      );
      const sortKey = getFieldKind(this.table.option.keySchema, "SortKey").at(
        0
      );

      if (hashKey && sortKey) {
        return `attribute_not_exists(${hashKey.field}) AND attribute_not_exists(${sortKey.field})`;
      } else if (hashKey.field) {
        return `attribute_not_exists(${hashKey.field})`;
      }
    }
  }
}
