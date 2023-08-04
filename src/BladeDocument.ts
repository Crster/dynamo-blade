import {
  DynamoDBDocumentClient,
  DeleteCommand,
  UpdateCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";

import { buildItem } from "./utils";
import BladeOption from "./BladeOption";
import BladeCollection from "./BladeCollection";
import { Model, UpdateValue } from "./BladeType";

export default class BladeDocument<Schema> {
  private option: BladeOption;

  constructor(option: BladeOption) {
    this.option = option;
  }

  open<T>(collection: T extends Model<Schema> ? T : Model<Schema>) {
    return new BladeCollection<Schema[typeof collection]>(
      this.option.openCollection(collection)
    );
  }

  toString() {
    return this.option.getFieldValue("PRIMARY_KEY");
  }

  async get(consistent?: boolean) {
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
      return buildItem<Schema>(result.Item, this.option);
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return null;
    }
  }

  setLater(value: UpdateValue<Schema>) {
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

    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
      },
      UpdateExpression: updateExpression.join("\n"),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
    });

    return command;
  }

  async set(values: UpdateValue<Schema>) {
    const docClient = DynamoDBDocumentClient.from(this.option.client);
    const command = this.setLater(values);

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
