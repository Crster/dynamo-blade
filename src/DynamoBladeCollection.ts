import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "./DynamoBlade";
import DynamoBladeDocument from "./DynamoBladeDocument";
import { buildKey, decodeNext } from "./utils/index";
import GetResult from "./GetResult";

export default class DynamoBladeCollection {
  private blade: DynamoBlade;
  private namespace: Array<string>;
  private name: string;

  constructor(blade: DynamoBlade, namespace: Array<string>, name: string) {
    this.blade = blade;
    this.namespace = namespace;
    this.name = name;
  }

  is(key: string) {
    return new DynamoBladeDocument(
      this.blade,
      [...this.namespace, this.name],
      key
    );
  }

  async get(next?: string): Promise<GetResult> {
    const { client, tableName, indexName } = this.blade.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const pkey = buildKey(this.blade, [...this.namespace, this.name]);

    const command = new QueryCommand({
      TableName: tableName,
      IndexName: pkey.useIndex ? indexName : undefined,
      KeyConditionExpression: `${pkey.hashKey.name} = :hashKey`,
      ExpressionAttributeValues: { ":hashKey": pkey.hashKey.value },
      ExclusiveStartKey: decodeNext(next),
    });

    const result = await docClient.send(command).catch((err) => err);
    return new GetResult(this.blade, result, pkey.collections);
  }

  addLater<T>(key: string, value: Partial<T>) {
    const { tableName, separator, indexName, hashKey, sortKey } =
      this.blade.option;

    const pkey = buildKey(this.blade, [
      ...this.namespace,
      this.name,
      `${separator}${key}`,
    ]);
    const command = new PutCommand({
      TableName: tableName,
      Item: {
        ...value,
        [pkey.hashKey.name]: pkey.hashKey.value,
        [pkey.sortKey.name]: pkey.sortKey.value,
        [`${indexName}${hashKey}`]: pkey.collections.join("."),
        [`${indexName}${sortKey}`]: String(key),
      },
    });

    return command;
  }

  async add<T>(key: string, value: Partial<T>): Promise<boolean> {
    const docClient = DynamoDBDocumentClient.from(this.blade.option.client);
    const command = this.addLater(key, value);

    const result = await docClient.send(command).catch((err) => err);
    return result.$metadata.httpStatusCode === 200;
  }

  async where(
    field: string,
    condition: string,
    value: any,
    next?: string
  ): Promise<GetResult> {
    const { client, tableName, separator, indexName } = this.blade.option;
    const pkey = buildKey(this.blade, [...this.namespace, this.name]);
    const docClient = DynamoDBDocumentClient.from(client);

    const filterCondition = [];
    const keyCondition = [`${pkey.hashKey.name} = :hashKey`];
    const keyValues = new Map();
    keyValues.set(":hashKey", pkey.hashKey.value);

    let pk = null;
    const fieldNames = new Map();
    if (field === pkey.sortKey.name) {
      if (condition == "begins_with") {
        keyCondition.push(`begins_with(${pkey.sortKey.name}, :sortKey)`);
      } else if (condition == "between") {
        keyCondition.push(
          `${pkey.sortKey.name} BETWEEN :sortKey01 AND :sortKey02`
        );
        if (!Array.isArray(value))
          throw new Error("Value should be an array of two value");
      } else {
        keyCondition.push(`${pkey.sortKey.name} ${condition} :sortKey`);
      }

      if (Array.isArray(value)) {
        if (pkey.useIndex) {
          keyValues.set(":sortKey01", String(value[0]));
          keyValues.set(":sortKey02", String(value[1]));
        } else {
          const skey01 = buildKey(this.blade, [
            ...this.namespace,
            this.name,
            `${separator}${value[0]}`,
          ]);

          const skey02 = buildKey(this.blade, [
            ...this.namespace,
            this.name,
            `${separator}${value[1]}`,
          ]);

          keyValues.set(":sortKey01", skey01.sortKey.value);
          keyValues.set(":sortKey02", skey02.sortKey.value);
        }
      } else {
        if (pkey.useIndex) {
          keyValues.set(":sortKey", String(value));
        } else {
          const skey = buildKey(this.blade, [
            ...this.namespace,
            this.name,
            `${separator}${value}`,
          ]);

          pk = String(value);
          keyValues.set(":sortKey", skey.sortKey.value);
        }
      }
    } else {
      if (!pkey.useIndex) {
        keyCondition.push(`begins_with(${pkey.sortKey.name}, :sortKey)`);
        keyValues.set(":sortKey", pkey.sortKey.value);
      }

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
    }

    const command = new QueryCommand({
      TableName: tableName,
      IndexName: pkey.useIndex ? indexName : undefined,
      KeyConditionExpression: keyCondition.join(" AND "),
      FilterExpression: filterCondition.length
        ? filterCondition.join(" AND ")
        : undefined,
      ExpressionAttributeNames:
        fieldNames.size > 0 ? Object.fromEntries(fieldNames) : undefined,
      ExpressionAttributeValues: Object.fromEntries(keyValues),
      ExclusiveStartKey: decodeNext(next),
      ScanIndexForward: !["<", ">", "<=", ">=", "between"].includes(condition),
    });

    const result = await docClient.send(command).catch((err) => err);
    return new GetResult(this.blade, result, pkey.collections, pk);
  }
}
