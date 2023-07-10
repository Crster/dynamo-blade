import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import { IResult } from "./IResult";
import DynamoBlade from "./DynamoBlade";
import DynamoBladeDocument from "./DynamoBladeDocument";
import { buildKey, buildResult, decodeNext, encodeNext } from "./utils/index";

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

  async get<T>(next?: string): Promise<IResult<T>> {
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

    const result = await docClient.send(command);
    return {
      item: buildResult<T>(this.blade, result.Items),
      next: encodeNext(result.LastEvaluatedKey),
    };
  }

  async add(key: string, value: any): Promise<boolean> {
    const { client, tableName, separator, indexName, hashKey, sortKey } =
      this.blade.option;
    const docClient = DynamoDBDocumentClient.from(client);

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

    const result = await docClient.send(command);
    return result.$metadata.httpStatusCode === 200;
  }

  async where<T>(
    field: string,
    condition: string,
    value?: any,
    next?: string
  ): Promise<IResult<T>> {
    const { client, tableName, indexName } = this.blade.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const pkey = buildKey(this.blade, [...this.namespace, this.name]);

    const filterCondition = [];
    const keyCondition = [`${pkey.hashKey.name} = :hashKey`];
    const keyValues = new Map();
    keyValues.set(":hashKey", pkey.hashKey.value);

    const fieldNames = new Map();
    if (field === pkey.sortKey.name) {
      if (condition == "begins_with") {
        keyCondition.push(`begins_with(${pkey.sortKey.name}, :sortKey)`);
      } else {
        keyCondition.push(`${pkey.sortKey.name} ${condition} :sortKey`);
      }
      keyValues.set(":sortKey", String(value));
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
      ExpressionAttributeNames: Object.fromEntries(fieldNames),
      ExpressionAttributeValues: Object.fromEntries(keyValues),
      ExclusiveStartKey: decodeNext(next),
      ScanIndexForward: !["<", ">", "<=", ">=", "between"].includes(condition)
    });

    const result = await docClient.send(command);
    return {
      item: buildResult<T>(this.blade, result.Items),
      next: encodeNext(result.LastEvaluatedKey),
    };
  }
}
