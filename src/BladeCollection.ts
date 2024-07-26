import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { decodeNext, buildItems, encodeNext, buildItem } from "./utils/index";
import BladeOption from "./BladeOption";
import BladeDocument from "./BladeDocument";
import { SimpleFilter, EntityField } from "./BladeType";
import BladeFilter from "./BladeFilter";

export default class BladeCollection<Schema> {
  private option: BladeOption;

  constructor(option: BladeOption) {
    this.option = option;
  }

  is(key: string) {
    return new BladeDocument<Schema>(this.option.openKey(key));
  }

  tail() {
    this.option.forwardScan = false;
    return this;
  }

  async get<T extends EntityField<Schema>>(next?: string) {
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
      ScanIndexForward: this.option.forwardScan,
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
      return buildItems<T>(
        result.Items,
        encodeNext(result.LastEvaluatedKey),
        this.option
      );
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return buildItems<T>([], null, this.option);
    }
  }

  async getAll<T extends keyof EntityField<Schema>>(
    collections: Array<T>,
    key: string,
    next?: string
  ) {
    const { client, tableName, separator, getFieldName, getFieldValue } =
      this.option.openKey(key);
    const docClient = DynamoDBDocumentClient.from(client);

    const input: QueryCommandInput = {
      TableName: tableName,
      KeyConditionExpression: `${getFieldName("HASH")} = :hashKey`,
      ExpressionAttributeValues: {
        ":hashKey": getFieldValue("PRIMARY_KEY"),
      },
      ExclusiveStartKey: decodeNext(next),
      ScanIndexForward: this.option.forwardScan,
    };

    try {
      const result = await docClient.send(new QueryCommand(input));
      const ret: Array<{ collection: T; data: any }> = [];

      for (const item of result.Items) {
        const sort: string = item[getFieldName("SORT")];

        const collectionName = sort.split(separator).at(0) as T;
        if (collections.includes(collectionName as any)) {
          ret.push({
            collection: collectionName,
            data: buildItem(item, this.option),
          });
        }
      }

      return ret;
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return null;
    }
  }

  addLater<T extends EntityField<Schema>>(key: string, value: Partial<T>) {
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

  async add<T extends EntityField<Schema>>(key: string, value: Partial<T>) {
    const command = this.addLater(key, value);

    const docClient = DynamoDBDocumentClient.from(this.option.client);
    try {
      const result = await docClient.send(command);
      return result.$metadata.httpStatusCode === 200 ? key : null;
    } catch (err) {
      console.warn(
        `Failed to add ${this.option.getFieldValue("PRIMARY_KEY")} (${
          err.message
        })`
      );
      return null;
    }
  }

  where<F extends keyof EntityField<Schema>>(
    field: F,
    condition: SimpleFilter,
    ...value: Array<EntityField<Schema>[F]>
  ) {
    return new BladeFilter<EntityField<Schema>>(this.option, field, condition, value);
  }
}
