import { BladeError } from "./BladeError";
import {
  BladeSchema,
  BladeTypeUpdate,
  DataFilter,
  PrimaryKeyConstructor,
  ValueFilter,
} from "./BladeType";

export function getDbKey(schema: BladeSchema, keys: Array<string>) {
  const ret: Record<string, string> = {};

  if (schema.table.hashKey) {
    ret[schema.table.hashKey] = "";

    for (let xx = 0; xx < keys.length - 2; xx++) {
      if ((xx + 1) % 2) {
        ret[schema.table.hashKey] += `${keys[xx]}#`;
      } else {
        ret[schema.table.hashKey] += `${keys[xx]}:`;
      }
    }
  }

  ret[schema.table.hashKey] = ret[schema.table.hashKey].slice(
    0,
    ret[schema.table.hashKey].length - 1
  );

  if (schema.table.sortKey) {
    ret[schema.table.sortKey] = "";

    for (let xx = keys.length - 2; xx < keys.length; xx++) {
      if ((xx + 1) % 2) {
        ret[schema.table.sortKey] += `${keys[xx]}#`;
      } else {
        ret[schema.table.sortKey] += `${keys[xx]}:`;
      }
    }

    ret[schema.table.sortKey] = ret[schema.table.sortKey].slice(
      0,
      ret[schema.table.sortKey].length - 1
    );
  }

  return ret;
}

export function getDbExtra(
  schema: BladeSchema,
  keys: Array<string>,
  operation: "ADD" | "UPDATE"
) {
  const ret: Record<string, string> = {};

  if (operation === "ADD") {
    let dataType: string;
    let currentSchema = schema.type as Record<string, any>;
    for (let xx = 0; xx < keys.length; xx++) {
      if ((xx + 1) % 2) {
        dataType = keys[xx];
        currentSchema = currentSchema[dataType].type;
        for (const k in currentSchema) {
          if (currentSchema[k] instanceof PrimaryKeyConstructor) {
            ret[k] = keys[xx + 1];
            break;
          }
        }
      }
    }

    if (schema.table.createdOn) {
      ret[schema.table.createdOn] = new Date().toISOString();
    }

    if (schema.table.typeKey) {
      ret[schema.table.typeKey] = dataType;
    }
  }

  if (schema.table.modifiedOn) {
    ret[schema.table.modifiedOn] = new Date().toISOString();
  }

  return ret;
}

export function getDbValue(
  schema: BladeSchema,
  keys: Array<string>,
  value: Record<string, any>
) {
  const ret = new Map<string, any>();
  let currentSchema = schema.type as Record<string, any>;
  for (let xx = 0; xx < keys.length; xx++) {
    if ((xx + 1) % 2) {
      currentSchema = currentSchema[keys[xx]].type;
    }
  }

  for (const k in currentSchema) {
    const val = value[k] ?? currentSchema[k]["default"];
    if (currentSchema[k] instanceof PrimaryKeyConstructor) {
      continue;
    } else if (val !== undefined) {
      ret.set(k, val);
    } else if (currentSchema[k]["required"]) {
      throw new BladeError("REQUIRED", `${k} must have a value`);
    }
  }

  return Object.fromEntries(ret);
}

export function getUpdateData<Schema extends BladeSchema>(
  schema: Schema,
  keys: Array<string>,
  value: BladeTypeUpdate<Schema>
) {
  const updateExpression: Array<string> = [];
  const attributeName = new Map<string, string>();
  const attributeValues = new Map<string, any>();

  let subValue: Record<string, any>;
  let setValue: Record<string, any> = {};

  const field = { counter: 0, set: [], add: [], remove: [], delete: [] };

  for (const key in value) {
    switch (key.toLocaleLowerCase()) {
      case "$set":
        setValue = { ...setValue, ...value[key] };
        break;
      case "$add":
        subValue = getDbValue(schema, keys, value[key]);
        for (const subKey in subValue) {
          field.add.push(`#prop${field.counter} :value${field.counter}`);
          attributeName.set(`#prop${field.counter}`, subKey);
          attributeValues.set(`:value${field.counter}`, subValue[subKey]);
          field.counter++;
        }
        break;
      case "$remove":
        subValue = getDbValue(schema, keys, value[key]);
        for (const subKey in subValue) {
          field.remove.push(`#prop${field.counter}`);
          attributeName.set(`#prop${field.counter}`, subValue[subKey]);
          field.counter++;
        }

        break;
      case "$delete":
        subValue = getDbValue(schema, keys, value[key]);
        for (const subKey in subValue) {
          field.delete.push(`#prop${field.counter} :value${field.counter}`);
          attributeName.set(`#prop${field.counter}`, subKey);
          attributeValues.set(`:value${field.counter}`, subValue[subKey]);
          field.counter++;
        }
        break;
      default:
        setValue[key] = value[key];
        break;
    }
  }

  setValue = {
    ...getDbValue(schema, keys, setValue),
    ...getDbExtra(schema, keys, "UPDATE"),
  };
  for (const key in setValue) {
    field.set.push(`#prop${field.counter}=:value${field.counter}`);
    attributeName.set(`#prop${field.counter}`, key);
    attributeValues.set(`:value${field.counter}`, setValue[key]);
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

  return {
    UpdateExpression: updateExpression.join("\n"),
    ExpressionAttributeNames: Object.fromEntries(attributeName),
    ExpressionAttributeValues: Object.fromEntries(attributeValues),
  };
}

export function getCondition(
  field: string,
  condition: ValueFilter | DataFilter,
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
    case "ATTRIBUTE_EXISTS":
      ret.filter.push(`attribute_exists(#field${counter})`);
      ret.field[`#field${counter}`] = field;
      break;
    case "ATTRIBUTE_NOT_EXISTS":
      ret.filter.push(`attribute_not_exists(#field${counter})`);
      ret.field[`#field${counter}`] = field;
      break;
    case "ATTRIBUTE_TYPE":
      ret.filter.push(`attribute_type(#field${counter}, :value${counter})`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value.at(0);
      break;
    case "CONTAINS":
      ret.filter.push(`contains(#field${counter}, :value${counter})`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value.at(0);
      break;
    case "SIZE":
      ret.filter.push(`size(#field${counter}) = :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value.at(0);
      break;
    case "SIZE_GT":
      ret.filter.push(`size(#field${counter}) < :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value.at(0);
      break;
    case "SIZE_LT":
      ret.filter.push(`size(#field${counter}) > :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value.at(0);
      break;
  }

  return ret;
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
