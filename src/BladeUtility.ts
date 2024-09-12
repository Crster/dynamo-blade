import { BladeError, BladeErrorCode } from "./BladeError";
import {
  BladeSchema,
  DynamoBladeOption,
  BladeItem,
  RequiredBladeItem,
  BladeSchemaKey,
  UpdateBladeItem,
  BladeItemType,
  BladeOperation,
  BladeViewCondition,
  BladeKey,
} from "./BladeType";

export function generateTimestamp(
  option: DynamoBladeOption,
  operation: BladeOperation
) {
  if (operation === "ADD") {
    if (option.schema.createdOn) {
      return {
        [typeof option.schema.createdOn === "boolean"
          ? "createdOn"
          : option.schema.createdOn]: new Date().toISOString(),
      };
    }
  } else if (operation === "SET") {
    if (option.schema.modifiedOn) {
      return {
        [typeof option.schema.modifiedOn === "boolean"
          ? "modifiedOn"
          : option.schema.modifiedOn]: new Date().toISOString(),
      };
    }
  }
}

export function generateKey<Schema extends BladeSchema>(
  option: DynamoBladeOption,
  key: BladeSchemaKey<Schema>,
  value: RequiredBladeItem<Schema>
) {
  const ret = {
    [option.schema.hashKey.field]: generateValue(
      "SET",
      option.schema.hashKey.type,
      key.hashKey(value)
    ),
  };

  if (option.schema.sortKey) {
    ret[option.schema.sortKey.field] = generateValue(
      "SET",
      option.schema.sortKey.type,
      key.sortKey(value)
    );
  }

  return ret;
}

export function generateValue(
  operation: BladeOperation,
  attribute: BladeItemType,
  value: any
) {
  if (attribute.name === "String") {
    value = String(value);
  } else if (attribute.name === "Number") {
    value = Number(value);
  } else if (attribute.name === "Boolean") {
    if (operation === "GET") {
      value = Boolean(value);
    } else {
      value = String(Boolean(value));
    }
  } else if (attribute.name === "Date") {
    if (operation === "GET") {
      value = new Date(value);
    } else {
      value = new Date(value).toISOString();
    }
  } else if (attribute.name === "Buffer") {
    value = Buffer.from(value);
  }

  return value;
}

export function getKey(option: DynamoBladeOption, indexKey: string) {
  let hashKey: BladeKey;
  let sortKey: BladeKey;

  if (indexKey) {
    const index = option.schema.index[indexKey];
    if (index.type === "LOCAL") {
      hashKey = option.schema.hashKey;
      if (index.sortKey) {
        sortKey = index.sortKey;
      }
    } else if (index.type === "GLOBAL") {
      hashKey = index.hashKey;
      if (index.sortKey) {
        sortKey = index.sortKey;
      }
    }
  } else {
    hashKey = option.schema.hashKey;
    if (option.schema.sortKey) {
      sortKey = option.schema.sortKey;
    }
  }

  return { hashKey, sortKey };
}

export function getCondition(
  field: string,
  condition: BladeViewCondition,
  value: any,
  counter: number
) {
  const ret = {
    filter: [],
    field: {},
    values: {},
  };

  switch (condition) {
    case "=":
      ret.filter.push(`#field${counter} = :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case "!=":
      ret.filter.push(`#field${counter} <> :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case "<":
      ret.filter.push(`#field${counter} < :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case "<=":
      ret.filter.push(`#field${counter} <= :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case ">":
      ret.filter.push(`#field${counter} > :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case ">=":
      ret.filter.push(`#field${counter} >= :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case "BETWEEN":
      ret.filter.push(
        `#field${counter} BETWEEN :valueFrom${counter} AND :valueTo${counter}`
      );
      ret.field[`#field${counter}`] = field;
      ret.values[`:valueFrom${counter}`] = value.at(0);
      ret.values[`:valueTo${counter}`] = value.at(1);
      break;
    case "IN":
      const values: Array<any> = [];
      value.forEach((val, index) => {
        values.push(`:value${index}`);
        ret.values[`:value${index}`] = val;
      });

      ret.filter.push(`#field${counter} IN (${values.join(",")})`);
      ret.field[`#field${counter}`] = field;
      break;
    case "BEGINS_WITH":
      ret.filter.push(`begins_with(#field${counter}, :value${counter})`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value.at(0);
      break;
  }

  return ret;
}

export function fillMap(map: Map<string, any>, record: Record<string, any>) {
  for (const k in record) {
    map.set(k, record[k]);
  }
}

export function generateItem<Schema extends BladeSchema>(
  schema: Schema,
  item: Record<string, any>,
  operation: BladeOperation
) {
  const ret = new Map<string, any>();

  if (item) {
    for (const k in schema) {
      let value: any = item[k];
      const attribute = schema[k];

      if (typeof attribute["value"] === "function") {
        const tmpValue = attribute["value"](operation, value);
        if (tmpValue !== undefined) {
          value = tmpValue;
        }
      }

      if (value !== undefined) {
        if (typeof attribute["type"] === "function") {
          if (attribute["type"] instanceof Array) {
            if (Array.isArray(value)) {
              const newValue = [];
              for (const ii of value) {
                newValue.push(
                  generateValue(
                    operation,
                    attribute["itemType"] as BladeItemType,
                    ii
                  )
                );
              }

              value = newValue;
            } else {
              value = undefined;
            }
          } else if (attribute["type"] instanceof Set) {
            if (value instanceof Set) {
              const newValue = new Set();
              for (const ii of value) {
                newValue.add(
                  generateValue(
                    operation,
                    attribute["itemType"] as BladeItemType,
                    ii
                  )
                );
              }

              value = newValue;
            } else if (Array.isArray(value)) {
              const newValue = new Set();
              for (const ii of value) {
                newValue.add(
                  generateValue(
                    operation,
                    attribute["itemType"] as BladeItemType,
                    ii
                  )
                );
              }

              value = newValue;
            } else {
              value = undefined;
            }
          } else if (attribute["type"] instanceof Map) {
            const newValue = new Map();
            if (value) {
              if (value instanceof Map) {
                for (const [k, v] of value) {
                  newValue.set(k, generateValue(operation, String, v));
                }
              } else if (value instanceof Object) {
                for (const k in value) {
                  newValue.set(k, generateValue(operation, String, value[k]));
                }
              }
            }

            if (newValue.size) {
              value = newValue;
            } else {
              value = undefined;
            }
          } else {
            value = generateValue(
              operation,
              attribute["type"] as BladeItemType,
              value
            );
          }
        } else {
          value = generateValue(operation, attribute as BladeItemType, value);
        }
      } else if (attribute["required"]) {
        throw new BladeError(
          BladeErrorCode.FieldIsRequired,
          `${k} is required`
        );
      }

      if (value !== undefined) {
        ret.set(k, value);
      }
    }
  }

  if (ret.size) {
    return Object.fromEntries(ret) as BladeItem<Schema>;
  }
}

export function generateUpdateItem<Schema extends BladeSchema>(
  schema: Schema,
  value: Record<string, any>,
  counter: number
) {
  const ret: Array<{
    prop: string;
    propKey: string;
    propValueKey: string;
    propValue: any;
  }> = [];
  let fieldCounter = counter || 0;

  const schemaKeys = Object.keys(schema);
  const tmpSchema = {};
  for (const key in value) {
    if (schemaKeys.includes(key)) {
      tmpSchema[key] = schema[key];
    }
  }

  const newValue = generateItem(tmpSchema, value, "SET");

  for (const key in newValue) {
    const propValue = newValue[key];
    ret.push({
      prop: key,
      propValue,
      propKey: `#prop${fieldCounter}`,
      propValueKey: `:value${fieldCounter}`,
    });
    fieldCounter++;
  }

  return ret;
}

export function generateUpdate<Schema extends BladeSchema>(
  schema: Schema,
  value: UpdateBladeItem<Schema>,
  extra?: Record<string, any>
) {
  const updateExpression: Array<string> = [];
  const expressionAttributeName = new Map<string, string>();
  const expressionAttributeValues = new Map<string, any>();

  const field = { counter: 0, set: [], add: [], remove: [], delete: [] };
  for (const key in value) {
    switch (key.toLocaleLowerCase()) {
      case "$set":
        for (const item of generateUpdateItem(
          schema,
          value[key],
          field.counter
        )) {
          field.set.push(`${item.propKey} = ${item.propValueKey}`);
          expressionAttributeName.set(item.propKey, item.prop);
          expressionAttributeValues.set(item.propValueKey, item.propValue);
          field.counter++;
        }
        break;
      case "$add":
        for (const item of generateUpdateItem(
          schema,
          value[key],
          field.counter
        )) {
          field.add.push(`${item.propKey} ${item.propValueKey}`);
          expressionAttributeName.set(item.propKey, item.prop);
          expressionAttributeValues.set(item.propValueKey, item.propValue);
          field.counter++;
        }
        break;
      case "$remove":
        for (const item of generateUpdateItem(
          schema,
          value[key],
          field.counter
        )) {
          field.remove.push(`${item.propKey}`);
          expressionAttributeName.set(item.propKey, item.prop);
          field.counter++;
        }

        break;
      case "$delete":
        for (const item of generateUpdateItem(
          schema,
          value[key],
          field.counter
        )) {
          field.delete.push(`${item.propKey} ${item.propValueKey}`);
          expressionAttributeName.set(item.propKey, item.prop);
          expressionAttributeValues.set(item.propValueKey, item.propValue);
          field.counter++;
        }
        break;
      default:
        for (const item of generateUpdateItem(
          schema,
          { [key]: value[key] },
          field.counter
        )) {
          field.set.push(`${item.propKey} = ${item.propValueKey}`);
          expressionAttributeName.set(item.propKey, item.prop);
          expressionAttributeValues.set(item.propValueKey, item.propValue);
          field.counter++;
        }
        break;
    }

    for (const key in extra) {
      field.set.push(`#prop${field.counter} = :value${field.counter}`);
      expressionAttributeName.set(`#prop${field.counter}`, key);
      expressionAttributeValues.set(`:value${field.counter}`, extra[key]);
      field.counter++;
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
  }

  return {
    UpdateExpression: updateExpression.join("\n"),
    ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
    ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
  };
}

export function generateScalarType(type: BladeItemType) {
  if (type instanceof Number) {
    return "N";
  } else if (type instanceof Buffer) {
    return "B";
  } else {
    return "S";
  }
}

export function decodeNext(next: string) {
  if (next) {
    try {
      return JSON.parse(Buffer.from(next, "base64").toString());
    } catch {
      throw new Error("Invalid next value");
    }
  }
}

export function encodeNext(next: any) {
  if (next) {
    return Buffer.from(JSON.stringify(next)).toString("base64");
  }
}
