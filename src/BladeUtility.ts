import {
  AttributeDefinition,
  ScalarAttributeType,
} from "@aws-sdk/client-dynamodb";
import { DataFilter, KeyFilter } from "./BladeView";
import { BladeField, BladeFieldKind } from "./BladeField";
import { BladeAttributeSchemaValueType } from "./BladeAttribute";

export function getCondition(
  field: string,
  condition: KeyFilter | DataFilter,
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
      ret.values[`:value${counter}`] = value;
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
      ret.values[`:value${counter}`] = value;
      break;
    case "CONTAINS":
      ret.filter.push(`contains(#field${counter}, :value${counter})`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case "SIZE":
      ret.filter.push(`size(#field${counter}) = :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case "SIZE_GT":
      ret.filter.push(`size(#field${counter}) < :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
    case "SIZE_LT":
      ret.filter.push(`size(#field${counter}) > :value${counter}`);
      ret.field[`#field${counter}`] = field;
      ret.values[`:value${counter}`] = value;
      break;
  }

  return ret;
}

export function fillMap(map: Map<string, any>, record: Record<string, any>) {
  for (const k in record) {
    map.set(k, record[k]);
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

export function addToAttributeDefinition(
  attributes: Array<AttributeDefinition>,
  name: string,
  type: ScalarAttributeType
) {
  if (!attributes.find((ii) => ii.AttributeName === name)) {
    attributes.push({
      AttributeName: name,
      AttributeType: type,
    });
  }
}

export function getFieldKind(
  keyFields: Record<string, BladeAttributeSchemaValueType>,
  kind: BladeFieldKind
) {
  let ret: Array<{
    field: string;
    type: ScalarAttributeType;
    bladeField: BladeField;
  }> = [];

  for (const field in keyFields) {
    const fieldKind = keyFields[field]["kind"];

    if (fieldKind === kind) {
      const bladeField = keyFields[field] as BladeField;
      let type: ScalarAttributeType = "S";

      switch (bladeField.type) {
        case Number:
          type = "N";
          break;
        case Buffer:
          type = "B";
          break;
      }

      ret.push({ bladeField, field, type });
    }
  }

  return ret;
}

export function BladeFilter(condition: KeyFilter, value: any) {
  return { condition, value };
}
