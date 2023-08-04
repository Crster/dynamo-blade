import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { decodeNext, buildItems, encodeNext } from "./utils/index";
import BladeOption from "./BladeOption";
import BladeDocument from "./BladeDocument";
import { FilterCondition, Model } from "./BladeType";

export default class BladeCollection<Schema> {
  private option: BladeOption;

  constructor(option: BladeOption) {
    this.option = option;
  }

  is(key: string) {
    return new BladeDocument<Schema>(this.option.openKey(key));
  }

  async get(next?: string) {
    const { client, tableName, isUseIndex, getFieldName, getFieldValue } =
      this.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const input: QueryCommandInput = {
      TableName: tableName,
      KeyConditionExpression: `${getFieldName(
        "HASH"
      )} = :hashKey AND begins_with(${getFieldName("SORT")}, :sortKey)`,
      ExpressionAttributeValues: {
        ":hashKey": getFieldValue("HASH"),
        ":sortKey": getFieldValue("SORT"),
      },
      ExclusiveStartKey: decodeNext(next),
    };

    // Using Index
    if (isUseIndex()) {
      input.IndexName = getFieldName("INDEX");
      input.KeyConditionExpression = `${getFieldName("HASH_INDEX")} = :hashKey`;
      input.ExpressionAttributeValues = {
        ":hashKey": getFieldValue("HASH"),
      };
    }

    try {
      const result = await docClient.send(new QueryCommand(input));
      return buildItems<Schema>(
        result.Items,
        encodeNext(result.LastEvaluatedKey),
        this.option
      );
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return buildItems<Schema>([], null, this.option)
    }
  }

  addLater(key: string, value: Partial<Schema>) {
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

  async add(key: string, value: Partial<Schema>) {
    const command = this.addLater(key, value);

    const docClient = DynamoDBDocumentClient.from(this.option.client);
    try {
      const result = await docClient.send(command);
      return result.$metadata.httpStatusCode === 200;
    } catch (err) {
      console.warn(
        `Failed to add ${this.option.getFieldValue("PRIMARY_KEY")} (${
          err.message
        })`
      );
      return false;
    }
  }

  async where<T>(
    field: T extends Model<Schema> ? T : Model<Schema>,
    condition: FilterCondition,
    value: any,
    next?: string
  ) {
    const {
      client,
      tableName,
      collection,
      isUseIndex,
      getFieldName,
      getFieldValue,
    } = this.option;
    const docClient = DynamoDBDocumentClient.from(client);

    const filterExpression: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, string>();

    const hashKey = isUseIndex()
      ? getFieldName("HASH_INDEX")
      : getFieldName("HASH");
    const sortKey = isUseIndex()
      ? getFieldName("SORT_INDEX")
      : getFieldName("SORT");

    const hashKeyValue = isUseIndex()
      ? getFieldValue("HASH_INDEX")
      : getFieldValue("HASH");

    // Build Key Condition
    keyConditionExpression.push(`${hashKey} = :hashKey`);
    expressionAttributeValues.set(":hashKey", hashKeyValue);

    // Build Filter Condition
    if (field !== sortKey) {
      if (!isUseIndex()) {
        keyConditionExpression.push(`begins_with(${sortKey}, :sortKey)`);
        expressionAttributeValues.set(":sortKey", collection);
      }
    }

    const filterCondition =
      field === sortKey ? keyConditionExpression : filterExpression;

    switch (condition) {
      case "=":
        filterCondition.push(`#field = :value`);
        expressionAttributeNames.set("#field", field as string);
        expressionAttributeValues.set(":value", value);
        break;
      case "!=":
        filterCondition.push(`#field <> :value`);
        expressionAttributeNames.set("#field", field as string);
        expressionAttributeValues.set(":value", value);
        break;
      case "<":
        filterCondition.push(`#field < :value`);
        expressionAttributeNames.set("#field", String(field));
        expressionAttributeValues.set(":value", value);
        break;
      case "<=":
        filterCondition.push(`#field <= :value`);
        expressionAttributeNames.set("#field", String(field));
        expressionAttributeValues.set(":value", value);
        break;
      case ">":
        filterCondition.push(`#field > :value`);
        expressionAttributeNames.set("#field", String(field));
        expressionAttributeValues.set(":value", value);
        break;
      case ">=":
        filterCondition.push(`#field >= :value`);
        expressionAttributeNames.set("#field", String(field));
        expressionAttributeValues.set(":value", value);
        break;
      case "BETWEEN":
        if (Array.isArray(value) && value.length === 2) {
          filterCondition.push(`#field BETWEEN :valueFrom AND :valueTo`);
          expressionAttributeNames.set("#field", String(field));
          expressionAttributeValues.set(":valueFrom", value.at(0));
          expressionAttributeValues.set(":valueTo", value.at(1));
        }
        break;
      case "IN":
        if (Array.isArray(value)) {
          const values: Array<any> = [];
          value.forEach((val, index) => {
            values.push(`:value${index}`);
            expressionAttributeValues.set(`:value${index}`, val);
          });

          filterCondition.push(`#field IN (${values.join(",")})`);
          expressionAttributeNames.set("#field", String(field));
        }
        break;
      case "BEGINS_WITH":
        filterCondition.push(`begins_with(#field, :value)`);
        expressionAttributeNames.set("#field", String(field));
        expressionAttributeValues.set(":value", value);
        break;
    }

    const command = new QueryCommand({
      TableName: tableName,
      IndexName: isUseIndex() ? getFieldName("INDEX") : undefined,
      KeyConditionExpression: keyConditionExpression.join(" AND "),
      FilterExpression:
        filterExpression.length > 0
          ? filterExpression.join(" AND ")
          : undefined,
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
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
      console.warn(`Failed to get ${collection} (${err.message})`);
      return buildItems<Schema>([], null, this.option)
    }
  }
}
