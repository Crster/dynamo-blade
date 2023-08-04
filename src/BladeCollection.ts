import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";

import BladeDocument from "./BladeDocument";
import { decodeNext, buildItems, encodeNext } from "./utils/index";
import BladeOption from "./BladeOption";
import { FilterCondition } from "./BladeType";

export default class BladeCollection<Schema> {
  private option: BladeOption;

  constructor(option: BladeOption) {
    this.option = option;
  }

  is(key: string) {
    return new BladeDocument<Schema>(this.option.openKey(key));
  }

  async get(next?: string) {
    const { client, tableName, getFieldName, getFieldValue } = this.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const command = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: `${getFieldName("HASH")} = :hashKey`,
      ExpressionAttributeValues: { ":hashKey": getFieldValue("HASH") },
      ExclusiveStartKey: decodeNext(next),
    });

    try {
      const result = await docClient.send(command);
      return buildItems<Schema>(
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

  addLater<T>(key: string, value: Partial<T>) {
    const { tableName, getFieldName, getFieldValue } = this.option.openKey(key);

    const command = new PutCommand({
      TableName: tableName,
      Item: {
        ...value,
        [getFieldName("HASH")]: getFieldValue("HASH"),
        [getFieldName("SORT")]: getFieldValue("SORT"),
        [getFieldName("HASH_INDEX")]: getFieldValue("HASH_INDEX"),
        [getFieldName("SORT_INDEX")]: getFieldValue("SORT_INDEX"),
      },
    });

    return command;
  }

  async add<T>(key: string, value: Partial<T>): Promise<boolean> {
    const command = this.addLater(key, value);

    const docClient = DynamoDBDocumentClient.from(this.option.client);
    const result = await docClient.send(command).catch((err) => err);
    return result.$metadata.httpStatusCode === 200;
  }

  async where(
    field: string,
    condition: FilterCondition,
    value: any,
    next?: string
  ) {
    const { client, tableName, collection, getFieldName, getFieldValue } =
      this.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const filterExpression: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressesionAttributeValues = new Map<string, string>();

    // Build Key Condition
    keyConditionExpression.push(`${getFieldName("HASH")} = :hashKey`);
    expressesionAttributeValues.set(":hashKey", getFieldValue("HASH"));

    // Build Filter Condition
    if (field !== getFieldName("SORT")) {
      keyConditionExpression.push(
        `begins_with(${getFieldName("SORT")}, :sortKey)`
      );
      expressesionAttributeValues.set(":sortKey", collection);
    }

    const filterCondition =
      field === getFieldName("SORT")
        ? keyConditionExpression
        : filterExpression;

    switch (condition) {
      case "=":
        filterCondition.push(`${field} = :value`);
        expressesionAttributeValues.set(":value", value);
        break;
      case "!=":
        filterCondition.push(`${field} <> :value`);
        expressesionAttributeValues.set(":value", value);
        break;
      case "<":
        filterCondition.push(`${field} < :value`);
        expressesionAttributeValues.set(":value", value);
        break;
      case "<=":
        filterCondition.push(`${field} <= :value`);
        expressesionAttributeValues.set(":value", value);
        break;
      case ">":
        filterCondition.push(`${field} > :value`);
        expressesionAttributeValues.set(":value", value);
        break;
      case ">=":
        filterCondition.push(`${field} >= :value`);
        expressesionAttributeValues.set(":value", value);
        break;
      case "BETWEEN":
        if (Array.isArray(value) && value.length === 2) {
          filterCondition.push(`${field} BETWEEN :valueFrom AND :valueTo`);
          expressesionAttributeValues.set(":valueFrom", value.at(0));
          expressesionAttributeValues.set(":valueTo", value.at(1));
        }
        break;
      case "IN":
        if (Array.isArray(value)) {
          const values: Array<any> = [];
          value.forEach((val, index) => {
            values.push(`:value${index}`);
            expressesionAttributeValues.set(`:value${index}`, val);
          });

          filterCondition.push(`${field} IN (${values.join(",")})`);
        }
        break;
      case "BEGINS_WITH":
        filterCondition.push(`begins_with(${field}, :value)`);
        expressesionAttributeValues.set(":value", value);
        break;
    }

    const command = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: keyConditionExpression.join(" AND "),
      FilterExpression:
        filterExpression.length > 0
          ? filterExpression.join(" AND ")
          : undefined,
      ExpressionAttributeValues: Object.fromEntries(
        expressesionAttributeValues
      ),
      ExclusiveStartKey: decodeNext(next),
    });

    try {
      const result = await docClient.send(command);
      return buildItems<Schema>(
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
}
