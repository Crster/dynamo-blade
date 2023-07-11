import {
  DynamoDBDocumentClient,
  QueryCommand,
  DeleteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "./DynamoBlade";
import DynamoBladeCollection from "./DynamoBladeCollection";
import { buildKey, decodeNext } from "./utils/index";
import GetResult from "./GetResult";

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

    if (pkey.sortKey.value && pkey.sortKey.value != pkey.hashKey.value) {
      keyConditions.push(`begins_with(${pkey.sortKey.name}, :sortKey)`);
      keyValues[":sortKey"] = pkey.sortKey.value;
    }

    const filterCondition = [];
    for (let index = 0; index < field?.length; index++) {
      filterCondition.push(`:fieldVal${index}`);
      keyValues[`:fieldVal${index}`] = `${[
        ...pkey.collections,
        field[index],
      ].join(".")}`;
    }

    const command = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: keyConditions.join(" AND "),
      FilterExpression: filterCondition.length
        ? `${indexName}${hashKey} IN (${filterCondition.join(", ")})`
        : undefined,
      ExpressionAttributeValues: keyValues,
      ExclusiveStartKey: decodeNext(next),
    });

    const result = await docClient.send(command);
    return new GetResult(this.blade, result, pkey.collections, this.key);
  }

  async set<T>(values: Partial<T>) {
    const { client, tableName, separator } = this.blade.option;
    const docClient = DynamoDBDocumentClient.from(client);

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
              val: values[prop][prop2],
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
              val: null,
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
          propValues.push({ prop, val: values[prop] });
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
      if (item.val) {
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

    const result = await docClient.send(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async remove() {
    const { client, tableName, separator } = this.blade.option;
    const docClient = DynamoDBDocumentClient.from(client);

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

    const result = await docClient.send(command);
    return result.$metadata.httpStatusCode === 200;
  }
}
