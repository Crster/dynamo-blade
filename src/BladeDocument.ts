import {
  DynamoDBDocumentClient,
  DeleteCommand,
  UpdateCommand,
  GetCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";

import BladeOption from "./BladeOption";
import BladeCollection from "./BladeCollection";
import { ConditionDefination, Entity, EntityField, UpdateValue } from './BladeType';
import { buildItem } from "./utils";

export default class BladeDocument<Schema> {
  private option: BladeOption;

  constructor(option: BladeOption) {
    this.option = option;
  }

  open<C extends keyof Entity<Schema>>(collection: C) {
    return new BladeCollection<Schema[C]>(
      this.option.openCollection(collection)
    );
  }

  toString() {
    return this.option.getFieldValue("PRIMARY_KEY");
  }

  async get<T extends EntityField<Schema>>(consistent?: boolean) {
    const { client, tableName, getFieldName, getFieldValue } = this.option;

    const command = new GetCommand({
      TableName: tableName,
      Key: {
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
      },
      ConsistentRead: consistent,
    });

    const docClient = DynamoDBDocumentClient.from(client);

    try {
      const result = await docClient.send(command);
      return buildItem<T>(result.Item, this.option);
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return null;
    }
  }

  validateLater<T extends EntityField<Schema>>(
    conditions: Array<ConditionDefination<keyof T>>
  ) {
    const { tableName, getFieldName, getFieldValue } = this.option;

    const expressionAttributeName = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    // Build Condition Expression
    const updateConditions = [];

    if (conditions && Array.isArray(conditions) && conditions.length > 0) {
      for (let xx = 0; xx < conditions.length; xx++) {
        const condition = conditions[xx];

        switch (condition.condition) {
          case "=":
            updateConditions.push(`#conField${xx} = :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              condition.field as string
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "!=":
            updateConditions.push(`#conField${xx} <> :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              condition.field as string
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "<":
            updateConditions.push(`#conField${xx} < :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "<=":
            updateConditions.push(`#conField${xx} <= :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case ">":
            updateConditions.push(`#conField${xx} > :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case ">=":
            updateConditions.push(`#conField${xx} >= :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "BETWEEN":
            if (
              Array.isArray(condition.value) &&
              condition.value.length === 2
            ) {
              updateConditions.push(
                `#conField${xx} BETWEEN :conValueFrom${xx} AND :conValueTo${xx}`
              );
              expressionAttributeName.set(
                `#conField${xx}`,
                String(condition.field)
              );
              expressionAttributeValues.set(
                `:conValueFrom${xx}`,
                condition.value.at(0)
              );
              expressionAttributeValues.set(
                `:conValueTo${xx}`,
                condition.value.at(1)
              );
            }
            break;
          case "IN":
            if (Array.isArray(condition.value)) {
              const values: Array<any> = [];
              condition.value.forEach((val, index) => {
                values.push(`:conValue${xx}${index}`);
                expressionAttributeValues.set(`:conValue${xx}${index}`, val);
              });

              updateConditions.push(
                `#conField${xx} IN (${condition.value.join(",")})`
              );
              expressionAttributeName.set(
                `#conField${xx}`,
                String(condition.field)
              );
            }
            break;
          case "BEGINS_WITH":
            updateConditions.push(
              `begins_with(#conField${xx}, :conValue${xx})`
            );
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "ATTRIBUTE_EXISTS":
            updateConditions.push(`attribute_exists(#conField${xx})`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            break;
          case "ATTRIBUTE_NOT_EXISTS":
            updateConditions.push(`attribute_not_exists(#conField${xx})`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            break;
          case "ATTRIBUTE_TYPE":
            updateConditions.push(
              `attribute_type(#conField${xx}, :conValue${xx})`
            );
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "CONTAINS":
            updateConditions.push(`contains(#conField${xx}, :conValue${xx})`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "SIZE":
            updateConditions.push(`size(#conField${xx}) = :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "SIZE_GT":
            updateConditions.push(`size(#conField${xx}) < :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "SIZE_LT":
            updateConditions.push(`size(#conField${xx}) > :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
        }
      }
    }

    const command = new QueryCommand({
      TableName: tableName,
      ExclusiveStartKey: {
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
      },
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      FilterExpression:
        updateConditions.length > 0
          ? updateConditions.join(" AND ")
          : undefined,
    });

    return command;
  }

  setLater<T extends EntityField<Schema>>(
    value: UpdateValue<T>,
    conditions?: Array<ConditionDefination<keyof T>>
  ) {
    const { tableName, getFieldName, getFieldValue } = this.option;

    const updateExpression: Array<string> = [];
    const expressionAttributeName = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    const field = { counter: 0, set: [], add: [], remove: [], delete: [] };
    for (const prop in value) {
      switch (prop.toLocaleLowerCase()) {
        case "$set":
          const setProps = value[prop];
          for (const subProp in setProps) {
            field.set.push(`#prop${field.counter} = :value${field.counter}`);
            expressionAttributeName.set(`#prop${field.counter}`, subProp);
            expressionAttributeValues.set(
              `:value${field.counter}`,
              setProps[subProp] != null ? setProps[subProp] : ""
            );
            field.counter += 1;
          }
          break;
        case "$add":
          const addProps = value[prop];
          for (const subProp in addProps) {
            field.add.push(`#prop${field.counter} :value${field.counter}`);
            expressionAttributeName.set(`#prop${field.counter}`, subProp);
            expressionAttributeValues.set(
              `:value${field.counter}`,
              addProps[subProp]
            );
            field.counter += 1;
          }
          break;
        case "$remove":
          const removeProps = value[prop];
          for (const subProp in removeProps) {
            if (removeProps[subProp]) {
              field.remove.push(`#prop${field.counter}`);
              expressionAttributeName.set(`#prop${field.counter}`, subProp);
            } else {
              field.set.push(`#prop${field.counter} = :value${field.counter}`);
              expressionAttributeName.set(`#prop${field.counter}`, subProp);
              expressionAttributeValues.set(`:value${field.counter}`, "");
            }
            field.counter += 1;
          }
          break;
        case "$delete":
          const deleteProps = value[prop];
          for (const subProp in deleteProps) {
            field.delete.push(`#prop${field.counter} :value${field.counter}`);
            expressionAttributeName.set(`#prop${field.counter}`, subProp);
            expressionAttributeValues.set(
              `:value${field.counter}`,
              deleteProps[subProp]
            );
            field.counter += 1;
          }
          break;
        default:
          field.set.push(`#prop${field.counter} = :value${field.counter}`);
          expressionAttributeName.set(`#prop${field.counter}`, prop);
          expressionAttributeValues.set(
            `:value${field.counter}`,
            value[prop] != null ? value[prop] : ""
          );
          field.counter += 1;
          break;
      }
    }

    // Populate item keys
    const keyValue = {
      [getFieldName("HASH_INDEX")]: getFieldValue("HASH_INDEX"),
      [getFieldName("SORT_INDEX")]: getFieldValue("SORT_INDEX"),
    };

    for (const prop in keyValue) {
      field.set.push(`#prop${field.counter} = :value${field.counter}`);
      expressionAttributeName.set(`#prop${field.counter}`, prop);
      expressionAttributeValues.set(`:value${field.counter}`, keyValue[prop]);
      field.counter += 1;
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

    // Build Condition Expression
    const updateConditions = [];

    if (conditions && Array.isArray(conditions) && conditions.length > 0) {
      for (let xx = 0; xx < conditions.length; xx++) {
        const condition = conditions[xx];

        switch (condition.condition) {
          case "=":
            updateConditions.push(`#conField${xx} = :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              condition.field as string
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "!=":
            updateConditions.push(`#conField${xx} <> :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              condition.field as string
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "<":
            updateConditions.push(`#conField${xx} < :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "<=":
            updateConditions.push(`#conField${xx} <= :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case ">":
            updateConditions.push(`#conField${xx} > :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case ">=":
            updateConditions.push(`#conField${xx} >= :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "BETWEEN":
            if (
              Array.isArray(condition.value) &&
              condition.value.length === 2
            ) {
              updateConditions.push(
                `#conField${xx} BETWEEN :conValueFrom${xx} AND :conValueTo${xx}`
              );
              expressionAttributeName.set(
                `#conField${xx}`,
                String(condition.field)
              );
              expressionAttributeValues.set(
                `:conValueFrom${xx}`,
                condition.value.at(0)
              );
              expressionAttributeValues.set(
                `:conValueTo${xx}`,
                condition.value.at(1)
              );
            }
            break;
          case "IN":
            if (Array.isArray(condition.value)) {
              const values: Array<any> = [];
              condition.value.forEach((val, index) => {
                values.push(`:conValue${xx}${index}`);
                expressionAttributeValues.set(`:conValue${xx}${index}`, val);
              });

              updateConditions.push(
                `#conField${xx} IN (${condition.value.join(",")})`
              );
              expressionAttributeName.set(
                `#conField${xx}`,
                String(condition.field)
              );
            }
            break;
          case "BEGINS_WITH":
            updateConditions.push(
              `begins_with(#conField${xx}, :conValue${xx})`
            );
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "ATTRIBUTE_EXISTS":
            updateConditions.push(`attribute_exists(#conField${xx})`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            break;
          case "ATTRIBUTE_NOT_EXISTS":
            updateConditions.push(`attribute_not_exists(#conField${xx})`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            break;
          case "ATTRIBUTE_TYPE":
            updateConditions.push(
              `attribute_type(#conField${xx}, :conValue${xx})`
            );
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "CONTAINS":
            updateConditions.push(`contains(#conField${xx}, :conValue${xx})`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "SIZE":
            updateConditions.push(`size(#conField${xx}) = :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "SIZE_GT":
            updateConditions.push(`size(#conField${xx}) < :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
          case "SIZE_LT":
            updateConditions.push(`size(#conField${xx}) > :conValue${xx}`);
            expressionAttributeName.set(
              `#conField${xx}`,
              String(condition.field)
            );
            expressionAttributeValues.set(`:conValue${xx}`, condition.value);
            break;
        }
      }
    }

    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
      },
      UpdateExpression: updateExpression.join("\n"),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      ConditionExpression:
        updateConditions.length > 0
          ? updateConditions.join(" AND ")
          : undefined,
    });

    return command;
  }

  async set<T extends EntityField<Schema>>(
    values: UpdateValue<T>,
    conditions?: Array<ConditionDefination<keyof T>>
  ) {
    const docClient = DynamoDBDocumentClient.from(this.option.client);
    const command = this.setLater(values, conditions);

    try {
      const result = await docClient.send(command);
      return result.$metadata.httpStatusCode === 200;
    } catch (err) {
      console.warn(
        `Failed to set ${this.option.getFieldValue("PRIMARY_KEY")} (${
          err.message
        })`
      );
      return false;
    }
  }

  removeLater() {
    const { tableName, getFieldName, getFieldValue } = this.option;

    const command = new DeleteCommand({
      TableName: tableName,
      Key: {
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
      },
    });

    return command;
  }

  async remove() {
    const docClient = DynamoDBDocumentClient.from(this.option.client);
    const command = this.removeLater();

    try {
      const result = await docClient.send(command);
      return result.$metadata.httpStatusCode === 200;
    } catch (err) {
      console.warn(
        `Failed to remove ${this.option.getFieldValue("PRIMARY_KEY")} (${
          err.message
        })`
      );
      return false;
    }
  }
}
