import { BladeError } from "./BladeError";
import {
  BladeOption,
  BladeSchema,
  BladeType,
  ValueFilter,
  BladeTypeUpdate,
  PrimaryKeyConstructor,
} from "./BladeType";
import { getCondition } from "./BladeUtility";
import { DataFilter } from "./BladeType";

interface KeyAttribute {
  dataType: string;
  primaryKey: string;
  value?: any;
  condition?: ValueFilter;
}

export class BladeKeySchema<Option extends BladeOption<BladeSchema>> {
  public readonly option: Option;

  private index: string;
  private scanIndexForward: boolean;
  private currentSchema: BladeType<any>;
  private dataType: string;
  private readonly keys: Array<KeyAttribute>;

  constructor(option: Option) {
    this.option = option;
    this.scanIndexForward = true;
    this.keys = [];
  }

  open(type: string) {
    if (this.currentSchema) {
      this.currentSchema = this.currentSchema[type]?.type;
    } else {
      this.currentSchema = this.option.schema.type[type]?.type;
    }

    if (!this.currentSchema) {
      throw new BladeError("TYPE_UNKNOWN", "Type not found");
    }

    let primaryKeyField: string;
    for (const key in this.currentSchema) {
      if (this.currentSchema[key] instanceof PrimaryKeyConstructor) {
        primaryKeyField = key;
        break;
      } else if (
        this.currentSchema[key]["type"] instanceof PrimaryKeyConstructor
      ) {
        primaryKeyField = key;
        break;
      }
    }

    if (!primaryKeyField) {
      throw new BladeError(
        "PRIMARY_KEY_UNKNOWN",
        "No primary key for the type " + type
      );
    }

    this.dataType = type;
    this.keys.push({
      dataType: type,
      primaryKey: primaryKeyField,
    });

    return this;
  }

  setIndex(index: string) {
    this.index = index;
    return this;
  }

  whereIndexKey(
    index: string,
    field: string,
    condition: ValueFilter,
    value: any
  ) {
    const currentIndex = this.option.schema.index[index];
    if (!currentIndex) throw new BladeError("INDEX_UNKNOWN", "Index not found");

    this.index = index;
    if (currentIndex.hashKey && currentIndex.hashKey[0] === field) {
      if (this.scanIndexForward) {
        this.scanIndexForward = ![">", "<", ">=", "<=", "BETWEEN"].includes(
          condition
        );
      }

      this.keys.push({
        dataType: "HASHKEY",
        primaryKey: currentIndex.hashKey[0],
        condition: condition,
        value,
      });

      return this;
    }

    if (currentIndex.sortKey && currentIndex.sortKey[0] === field) {
      if (this.scanIndexForward) {
        this.scanIndexForward = ![">", "<", ">=", "<=", "BETWEEN"].includes(
          condition
        );
      }

      this.keys.push({
        dataType: "SORTKEY",
        primaryKey: currentIndex.sortKey[0],
        condition: condition,
        value,
      });

      return this;
    }

    return this;
  }

  whereKey(condition: ValueFilter, value: any) {
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
    return this.option.schema.table.name;
  }

  getKey() {
    return this.keys;
  }

  getKeyValue() {
    const ret = {};

    let hashKey = this.option.schema.table.hashKey;
    let sortKey = this.option.schema.table.sortKey;

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

        if (ret[hashKey]) {
          ret[hashKey] += `${key.dataType}#${key.value}:`;
        } else {
          ret[hashKey] = `${key.dataType}#${key.value}:`;
        }
      }

      ret[hashKey] = ret[hashKey].slice(0, ret[hashKey].length - 1);

      for (let xx = sortKeyRange[0]; xx < sortKeyRange[1]; xx++) {
        const key = this.keys[xx];

        if (ret[sortKey]) {
          ret[sortKey] += `${key.dataType}#${key.value}:`;
        } else {
          ret[sortKey] = `${key.dataType}#${key.value}:`;
        }
      }

      ret[sortKey] = ret[sortKey].slice(0, ret[sortKey].length - 1);
    } else if (hashKey) {
      for (let xx = 0; xx < this.keys.length; xx++) {
        const key = this.keys[xx];

        if (ret[hashKey]) {
          ret[hashKey] += `${key.dataType}#${key.value}:`;
        } else {
          ret[hashKey] = `${key.dataType}#${key.value}:`;
        }
      }

      ret[hashKey] = ret[hashKey].slice(0, ret[hashKey].length - 1);
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

    let hashKey = this.option.schema.table.hashKey;
    let sortKey = this.option.schema.table.sortKey;

    if (this.index) {
      const currentIndex = this.option.schema.index[this.index];
      hashKey = currentIndex.hashKey[0];
      sortKey = currentIndex.sortKey[0];
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

      let condition: ValueFilter;
      for (let xx = hashKeyRange[0]; xx < hashKeyRange[1]; xx++) {
        const key = this.keys[xx];
        condition = key.condition;
        keyValue.hashKey.push(this.getValue(key.dataType, key.value));
      }

      ret.push({
        dataType: "HASHKEY",
        primaryKey: hashKey,
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
            primaryKey: sortKey,
            condition: condition,
            value:
              keyValue.sortKey.length === 1
                ? keyValue.sortKey[0]
                : keyValue.sortKey.join(":"),
          });
        }
      }
    } else if (hashKey) {
      let condition: ValueFilter;
      for (let xx = 0; xx < this.keys.length; xx++) {
        const key = this.keys[xx];
        condition = key.condition;
        keyValue.hashKey.push(this.getValue(key.dataType, key.value));
      }

      ret.push({
        dataType: "HASHKEY",
        primaryKey: hashKey,
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
    const schema = this.currentSchema[field];
    let fieldValue: any;

    if (typeof schema === "function" && schema.name === field) {
      fieldValue = schema(value);
    } else {
      fieldValue = value[field] ?? schema["default"];
    }

    if (fieldValue instanceof Date) {
      return fieldValue.toISOString();
    }

    return fieldValue;
  }

  getNewItem(value: Record<string, any>) {
    const ret = {};

    const dbKey = this.getKeyValue();
    for (const field in dbKey) {
      ret[field] = dbKey[field];
    }

    for (const key of this.keys) {
      ret[key.primaryKey] = key.value;
    }

    if (this.option.schema.table.typeKey) {
      ret[this.option.schema.table.typeKey] = this.dataType;
    }

    if (this.option.schema.table.createdOn) {
      ret[this.option.schema.table.createdOn] = new Date().toISOString();
    }

    if (this.option.schema.table.modifiedOn) {
      ret[this.option.schema.table.modifiedOn] = new Date().toISOString();
    }

    const tmpValue = { ...ret, ...value };
    for (const field in this.currentSchema) {
      const fieldValue = this.getItemValue(tmpValue, field);

      if (fieldValue !== undefined) {
        ret[field] = fieldValue;
      }
    }

    return ret;
  }

  getUpdateItem(
    value: BladeTypeUpdate<BladeType<any>>,
    condition?: [string, ValueFilter | DataFilter, any]
  ) {
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

    if (this.option.schema.table.typeKey) {
      updates.set(this.option.schema.table.typeKey, {
        value: this.dataType,
        type: "$set",
      });
    }

    if (this.option.schema.table.modifiedOn) {
      updates.set(this.option.schema.table.modifiedOn, {
        value: new Date().toISOString(),
        type: "$set",
      });
    }

    const tmpValue = {};
    for (const [k, v] of updates) {
      tmpValue[k] = v.value;
    }

    for (const field in this.currentSchema) {
      const schema = this.currentSchema[field];
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
    return this.option.client.send(command);
  }

  buildItem<Type>(value: Record<string, any>) {
    if (value) {
      const ret = {};

      for (const field in this.currentSchema) {
        const schema = this.currentSchema[field];
        const fieldValue = value[field] ?? schema["default"];

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
      if (
        this.option.schema.table.hashKey &&
        this.option.schema.table.sortKey
      ) {
        return `attribute_not_exists(${this.option.schema.table.hashKey}) AND attribute_not_exists(${this.option.schema.table.sortKey})`;
      } else if (this.option.schema.table.hashKey) {
        return `attribute_not_exists(${this.option.schema.table.hashKey})`;
      }
    }
  }
}
