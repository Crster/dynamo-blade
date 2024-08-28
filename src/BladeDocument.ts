import {
  DeleteCommand,
  UpdateCommand,
  GetCommand,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

import BladeOption from "./BladeOption";
import BladeCollection from "./BladeCollection";
import {
  Condition,
  CollectionName,
  UpdateValue,
  BladeItem,
  Option,
} from "./BladeType";
import {
  buildCondition,
  buildItem,
  buildItems,
  decodeNext,
  encodeNext,
} from "./utils";

export default class BladeDocument<
  Opt extends Option,
  Collection extends keyof Opt["schema"]
> {
  private option: BladeOption<Opt>;

  constructor(option: BladeOption<Opt>) {
    this.option = option;
  }

  open<C extends CollectionName<Opt>>(collection: C) {
    return new BladeCollection<Opt, Collection>(
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

    try {
      const result = await client.send(command);
      return buildItem<Opt, Collection>(result.Item, this.option);
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return null;
    }
  }

  async getWith<T extends CollectionName<Opt>>(
    collections: Array<T>,
    next?: string
  ) {
    const { client, tableName, separator, getFieldName, getFieldValue } =
      this.option;

    const filterConditionExpression = collections.map(
      (value, index) => `${getFieldName("HASH_INDEX")} = :sortIndex${index}`
    );
    const filterConditionValues = collections.reduce((prev, curr, index) => {
      prev[`:sortIndex${index}`] = `${getFieldValue("HASH_INDEX")}:${String(
        curr
      )}`;
      return prev;
    }, {});

    if (collections.length) {
      filterConditionExpression.push(
        `${getFieldName("HASH_INDEX")} = :sortIndex${collections.length}`
      );
      filterConditionValues[`:sortIndex${collections.length}`] =
        getFieldValue("HASH_INDEX");
    }

    const input: QueryCommandInput = {
      TableName: tableName,
      KeyConditionExpression: `${getFieldName("HASH")} = :hashKey`,
      ExpressionAttributeValues: {
        ":hashKey": getFieldValue("PRIMARY_KEY"),
        ...filterConditionValues,
      },
      FilterExpression: filterConditionExpression.length
        ? filterConditionExpression.join(" OR ")
        : undefined,
      ExclusiveStartKey: decodeNext(next),
      ScanIndexForward: this.option.forwardScan,
    };

    try {
      const result = await client.send(new QueryCommand(input));
      return buildItems<Opt, Collection>(
        result.Items,
        encodeNext(result.LastEvaluatedKey),
        this.option
      );
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return null;
    }
  }

  when(conditions: Array<Condition>) {
    const { tableName, getFieldName, getFieldValue } = this.option;

    const expressionAttributeName = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    // Build Condition Expression
    const filterExpression = buildCondition(
      expressionAttributeName,
      expressionAttributeValues,
      conditions
    );

    const command = new QueryCommand({
      TableName: tableName,
      ExclusiveStartKey: {
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
      },
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      FilterExpression: filterExpression,
    });

    return command;
  }

  setLater(
    value: UpdateValue<BladeItem<Opt, Collection>>,
    conditions?: Array<Condition>
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
    const conditionExpression = buildCondition(
      expressionAttributeName,
      expressionAttributeValues,
      conditions
    );

    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
      },
      UpdateExpression: updateExpression.join("\n"),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeName),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      ConditionExpression: conditionExpression,
    });

    return command;
  }

  async set(
    value: UpdateValue<BladeItem<Opt, Collection>>,
    conditions?: Array<Condition>
  ) {
    const command = this.setLater(value, conditions);

    try {
      const result = await this.option.client.send(command);
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
    const command = this.removeLater();

    try {
      const result = await this.option.client.send(command);
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
