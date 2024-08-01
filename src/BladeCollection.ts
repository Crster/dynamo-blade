import {
  QueryCommand,
  PutCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { decodeNext, buildItems, encodeNext, buildItem } from "./utils/index";
import BladeOption from "./BladeOption";
import BladeDocument from "./BladeDocument";
import {
  ValueFilter,
  Item,
  ItemSchema,
  BladeItem,
} from "./BladeType";
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

  async get(next?: string) {
    const { client, tableName, isUseIndex, getFieldName, getFieldValue } =
      this.option;

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
      const result = await client.send(new QueryCommand(input));
      return buildItems<BladeItem<Schema>>(
        result.Items,
        encodeNext(result.LastEvaluatedKey),
        this.option
      );
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return buildItems<BladeItem<Schema>>([], null, this.option);
    }
  }

  addLater(key: string, value: Item<Schema>) {
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

  async add(key: string, value: Item<Schema>) {
    const command = this.addLater(key, value);
    try {
      const result = await this.option.client.send(command);
      if (result.$metadata.httpStatusCode === 200) {
        return buildItem<BladeItem<Schema>>(command.input.Item, this.option);
      }
    } catch (err) {
      console.warn(
        `Failed to add ${this.option.getFieldValue("PRIMARY_KEY")} (${
          err.message
        })`
      );
    }
  }

  where<F extends keyof ItemSchema<Schema>>(
    field: F,
    condition: ValueFilter,
    value: ItemSchema<Schema>[F] | Array<ItemSchema<Schema>[F]>
  ) {
    return new BladeFilter<Schema>(
      this.option,
      field,
      condition,
      Array.isArray(value) ? value : [value]
    );
  }
}
