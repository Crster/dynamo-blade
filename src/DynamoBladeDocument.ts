import {
  DynamoDBDocumentClient,
  QueryCommand,
  DeleteCommand,
  UpdateCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "./DynamoBlade";
import DynamoBladeCollection from "./DynamoBladeCollection";
import { buildKey, decodeNext } from "./utils/index";
import GetResult from "./GetResult";
import { ConditionCheck } from "@aws-sdk/client-dynamodb";

export default class DynamoBladeDocument {
  private blade: DynamoBlade;
  private namespace: Array<string>;
  private key: string;

  constructor(blade: DynamoBlade, namespace: Array<string>, key: string) {
    this.blade = blade;
    this.namespace = namespace;
    this.key = key;
  }

  open(collection: string) {
    return new DynamoBladeCollection(
      this.blade,
      [...this.namespace, `${this.blade.option.separator}${this.key}`],
      collection
    );
  }

  toString() {
    const pkey = buildKey(this.blade, [
      ...this.namespace,
      `${this.blade.option.separator}${this.key}`,
    ]);

    return pkey.sortKey.value;
  }

  when(field: string, condition: string, value: any) {
    const { tableName, separator } = this.blade.option;

    const pkey = buildKey(this.blade, [
      ...this.namespace,
      `${separator}${this.key}`,
    ]);

    const filterCondition = [];
    const keyValues = new Map();
    const fieldNames = new Map();

    if (field && condition && value) {
      if (
        ["begins_with", "contains", "size", "attribute_type"].includes(
          condition
        )
      ) {
        filterCondition.push(`${condition}(#fieldName, :fieldValue)`);
        keyValues.set(":fieldValue", value);
      } else if (
        ["attribute_not_exists", "attribute_exists"].includes(condition)
      ) {
        filterCondition.push(`${condition}(#fieldName)`);
      } else if (condition == "between") {
        filterCondition.push(
          `#fieldName BETWEEN :fieldValue01 AND :fieldValue02`
        );
        if (Array.isArray(value)) {
          keyValues.set(":fieldValue01", value[0]);
          keyValues.set(":fieldValue02", value[1]);
        } else {
          throw new Error("Value should be an array of two value");
        }
      } else if (condition == "in") {
        const fieldInName = [];
        for (let index = 0; index < value.length; index++) {
          fieldInName.push(`:fieldValueIn${index}`);
          keyValues.set(`:fieldValueIn${index}`, value?.at(index));
        }

        filterCondition.push(`#fieldName IN (${fieldInName.join(",")})`);
      } else {
        filterCondition.push(`#fieldName ${condition} :fieldValue`);
        keyValues.set(":fieldValue", value);
      }

      fieldNames.set("#fieldName", field);
    } else {
      filterCondition.push(`attribute_exists(${pkey.sortKey.name})`);
    }

    const command: ConditionCheck = {
      TableName: tableName,
      Key: {
        [pkey.hashKey.name]: { S: pkey.hashKey.value },
        [pkey.sortKey.name]: { S: pkey.sortKey.value },
      },
      ConditionExpression: filterCondition.join(" AND "),
      ExpressionAttributeNames:
        fieldNames.size > 0 ? Object.fromEntries(fieldNames) : undefined,
      ExpressionAttributeValues:
        keyValues.size > 0 ? Object.fromEntries(keyValues) : undefined,
    };

    return command;
  }

  async get(field?: Array<string>, next?: string): Promise<GetResult> {
    if (typeof field === "string") {
      next = field;
      field = [];
    }

    const { client, tableName, separator, indexName, hashKey } =
      this.blade.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const pkey = buildKey(this.blade, [
      ...this.namespace,
      `${separator}${this.key}`,
    ]);

    const keyConditions = [];
    const keyValues = {};

    if (pkey.hashKey.value) {
      keyConditions.push(`${pkey.hashKey.name} = :hashKey`);
      keyValues[":hashKey"] = pkey.hashKey.value;
    }

    const filterCondition = [];
    for (let index = 0; index < field?.length; index++) {
      filterCondition.push(`:fieldVal${index}`);

      if (field[index]) {
        keyValues[`:fieldVal${index}`] = `${[
          ...pkey.collections,
          field[index],
        ].join(".")}`;
      } else {
        keyValues[`:fieldVal${index}`] = pkey.collections.join(".");
      }
    }

    let useGet = false;
    if (pkey.sortKey.value && pkey.sortKey.value != pkey.hashKey.value) {
      if (filterCondition.length > 0) {
        keyConditions.push(`begins_with(${pkey.sortKey.name}, :sortKey)`);
      } else {
        useGet = !pkey.useIndex;
        keyConditions.push(`${pkey.sortKey.name} = :sortKey`);
      }
      keyValues[":sortKey"] = pkey.sortKey.value;
    }

    let command = null;
    if (useGet) {
      command = new GetCommand({
        TableName: tableName,
        ConsistentRead: true,
        Key: {
          [pkey.hashKey.name]: pkey.hashKey.value,
          [pkey.sortKey.name]: pkey.sortKey.value,
        },
      });
    } else {
      command = new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: keyConditions.join(" AND "),
        FilterExpression: filterCondition.length
          ? `${indexName}${hashKey} IN (${filterCondition.join(", ")})`
          : undefined,
        ExpressionAttributeValues: keyValues,
        ExclusiveStartKey: decodeNext(next),
      });
    }

    const result = await docClient.send(command).catch((err) => err);
    return new GetResult(this.blade, result, pkey.collections, this.key);
  }

  setLater<T>(values: Partial<T>) {
    const { tableName, separator } = this.blade.option;

    const pkey = buildKey(this.blade, [
      ...this.namespace,
      `${separator}${this.key}`,
    ]);

    const setValues = [];
    const addValues = [];
    const delValues = [];
    const remValues = [];
    const propValues = [];

    for (const prop in values) {
      switch (prop.toLowerCase()) {
        case "$set":
          for (const prop2 in values[prop]) {
            setValues.push(
              `#prop${propValues.length} = :val${propValues.length}`
            );
            propValues.push({
              prop: prop2,
              val: values[prop][prop2] != null ? values[prop][prop2] : "",
            });
          }
          break;
        case "$add":
          for (const prop2 in values[prop]) {
            addValues.push(
              `#prop${propValues.length} :val${propValues.length}`
            );
            propValues.push({
              prop: prop2,
              val: values[prop][prop2],
            });
          }
          break;
        case "$remove":
          for (const prop2 in values[prop]) {
            remValues.push(`#prop${propValues.length}`);
            propValues.push({
              prop: prop2,
              val: "",
              remove: true,
            });
          }
          break;
        case "$delete":
          for (const prop2 in values[prop]) {
            delValues.push(
              `#prop${propValues.length} :val${propValues.length}`
            );
            propValues.push({
              prop: prop2,
              val: values[prop][prop2],
            });
          }
          break;
        default:
          setValues.push(
            `#prop${propValues.length} = :val${propValues.length}`
          );
          propValues.push({
            prop,
            val: values[prop] != null ? values[prop] : "",
          });
          break;
      }
    }

    let UpdateExpression = [];
    if (setValues.length > 0) {
      UpdateExpression.push(`SET ${setValues.join(",")}`);
    }

    if (addValues.length > 0) {
      UpdateExpression.push(`ADD ${addValues.join(" ")}`);
    }

    if (delValues.length > 0) {
      UpdateExpression.push(`DELETE ${delValues.join(" ")}`);
    }

    if (remValues.length > 0) {
      UpdateExpression.push(`REMOVE ${remValues.join(" ")}`);
    }

    let ExpressionAttributeValues = {};
    let ExpressionAttributeNames = {};
    propValues.forEach((item, index) => {
      if (!item.remove) {
        ExpressionAttributeValues[`:val${index}`] = item.val;
      }

      ExpressionAttributeNames[`#prop${index}`] = item.prop;
    });

    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        [pkey.hashKey.name]: pkey.hashKey.value,
        [pkey.sortKey.name]: pkey.sortKey.value,
      },
      UpdateExpression: UpdateExpression.join(" "),
      ExpressionAttributeNames,
      ExpressionAttributeValues:
        Object.keys(ExpressionAttributeValues).length > 0
          ? ExpressionAttributeValues
          : undefined,
    });

    return command;
  }

  async set<T>(values: Partial<T>) {
    const docClient = DynamoDBDocumentClient.from(this.blade.option.client);
    const command = this.setLater(values);

    const result = await docClient.send(command).catch((err) => err);
    return result.$metadata.httpStatusCode === 200;
  }

  removeLater() {
    const { tableName, separator } = this.blade.option;

    const pkey = buildKey(this.blade, [
      ...this.namespace,
      `${separator}${this.key}`,
    ]);

    const command = new DeleteCommand({
      TableName: tableName,
      Key: {
        [pkey.hashKey.name]: pkey.hashKey.value,
        [pkey.sortKey.name]: pkey.sortKey.value,
      },
    });

    return command;
  }

  async remove() {
    const docClient = DynamoDBDocumentClient.from(this.blade.option.client);
    const command = this.removeLater();

    const result = await docClient.send(command).catch((err) => err);
    return result.$metadata.httpStatusCode === 200;
  }
}
