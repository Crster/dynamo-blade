import {
  DeleteCommand,
  UpdateCommand,
  GetCommand,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

import {
  Condition,
  CollectionName,
  UpdateValue,
  CollectionSchema,
  Option,
  CollectionSchemaKey,
  BladeItem,
} from "./BladeType";
import { buildCondition, buildItems, decodeNext, encodeNext } from "./utils";

export default class BladeDocument<
  Opt extends Option,
  Collection extends string & keyof Opt["schema"]
> {
  private option: Opt;
  private collection: Collection;
  private key: CollectionSchemaKey<Opt, Collection>;

  constructor(
    option: Opt,
    collection: Collection,
    key: CollectionSchemaKey<Opt, Collection>
  ) {
    this.option = option;
    this.collection = collection;
    this.key = key;
  }

  async get(consistent?: boolean) {
    const { client, tableName, primaryKey, schema } = this.option;

    const key = new Map<string, any>();
    if (primaryKey.hashKey) {
      key.set(
        primaryKey.hashKey[0],
        schema[this.collection]["hashKey"](this.key)
      );
    }
    if (primaryKey.sortKey) {
      key.set(
        primaryKey.sortKey[0],
        schema[this.collection]["sortKey"](this.key)
      );
    }

    const command = new GetCommand({
      TableName: tableName,
      Key: Object.fromEntries(key),
      ConsistentRead: consistent,
    });

    const result = await client.send(command);
    return schema[this.collection].buildItem<
      BladeItem<Opt["schema"][Collection]>
    >(result.Item);
  }

  when(conditions: Array<Condition>) {
    const { tableName, primaryKey, schema } = this.option;

    const expressionAttributeName = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    // Build Condition Expression
    const filterExpression = buildCondition(
      expressionAttributeName,
      expressionAttributeValues,
      conditions
    );

    const key = new Map<string, any>();
    if (primaryKey.hashKey) {
      key.set(
        primaryKey.hashKey[0],
        schema[this.collection]["hashKey"](this.key)
      );
    }
    if (primaryKey.sortKey) {
      key.set(
        primaryKey.sortKey[0],
        schema[this.collection]["sortKey"](this.key)
      );
    }

    const command = new QueryCommand({
      TableName: tableName,
      ExclusiveStartKey: Object.fromEntries(key),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      FilterExpression: filterExpression,
    });

    return command;
  }

  setLater(
    value: UpdateValue<CollectionSchema<Opt, Collection>>,
    conditions?: Array<Condition>
  ) {
    const { tableName, primaryKey, schema } = this.option;

    const updateExpression: Array<string> = [];
    const expressionAttributeName = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    const field = { counter: 0, set: [], add: [], remove: [], delete: [] };
    for (const prop in value) {
      switch (prop.toLocaleLowerCase()) {
        case "$set":
          const setProps = value[prop];
          for (const subProp in setProps) {
            const val = schema[this.collection].getItemValue(
              prop,
              setProps[subProp]
            );

            if (val !== undefined) {
              field.set.push(`#prop${field.counter} = :value${field.counter}`);
              expressionAttributeName.set(`#prop${field.counter}`, subProp);
              expressionAttributeValues.set(`:value${field.counter}`, val);
              field.counter += 1;
            }
          }
          break;
        case "$add":
          const addProps = value[prop];
          for (const subProp in addProps) {
            const val = schema[this.collection].getItemValue(
              prop,
              addProps[subProp]
            );

            if (val !== undefined) {
              field.add.push(`#prop${field.counter} :value${field.counter}`);
              expressionAttributeName.set(`#prop${field.counter}`, subProp);
              expressionAttributeValues.set(`:value${field.counter}`, val);
              field.counter += 1;
            }
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
            const val = schema[this.collection].getItemValue(
              prop,
              deleteProps[subProp]
            );

            if (val !== undefined) {
              field.delete.push(`#prop${field.counter} :value${field.counter}`);
              expressionAttributeName.set(`#prop${field.counter}`, subProp);
              expressionAttributeValues.set(`:value${field.counter}`, val);
              field.counter += 1;
            }
          }
          break;
        default:
          const val = schema[this.collection].getItemValue(prop, value[prop]);

          if (val !== undefined) {
            field.set.push(`#prop${field.counter} = :value${field.counter}`);
            expressionAttributeName.set(`#prop${field.counter}`, prop);
            expressionAttributeValues.set(`:value${field.counter}`, val);
            field.counter += 1;
          }
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

    // Build Condition Expression
    const conditionExpression = buildCondition(
      expressionAttributeName,
      expressionAttributeValues,
      conditions
    );

    const key = new Map<string, any>();
    if (primaryKey.hashKey) {
      key.set(
        primaryKey.hashKey[0],
        schema[this.collection]["hashKey"](this.key)
      );
    }
    if (primaryKey.sortKey) {
      key.set(
        primaryKey.sortKey[0],
        schema[this.collection]["sortKey"](this.key)
      );
    }

    const command = new UpdateCommand({
      TableName: tableName,
      Key: Object.fromEntries(key),
      UpdateExpression: updateExpression.join("\n"),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      ConditionExpression: conditionExpression,
    });

    return command;
  }

  async set(
    value: UpdateValue<CollectionSchema<Opt, Collection>>,
    conditions?: Array<Condition>
  ) {
    const command = this.setLater(value, conditions);

    const result = await this.option.client.send(command);
    return result.$metadata.httpStatusCode === 200;
  }

  removeLater() {
    const { tableName, primaryKey, schema } = this.option;

    const key = new Map<string, any>();
    if (primaryKey.hashKey) {
      key.set(
        primaryKey.hashKey[0],
        schema[this.collection]["hashKey"](this.key)
      );
    }
    if (primaryKey.sortKey) {
      key.set(
        primaryKey.sortKey[0],
        schema[this.collection]["sortKey"](this.key)
      );
    }

    const command = new DeleteCommand({
      TableName: tableName,
      Key: Object.fromEntries(key),
    });

    return command;
  }

  async remove() {
    const command = this.removeLater();
    const result = await this.option.client.send(command);
    return result.$metadata.httpStatusCode === 200;
  }
}
