import {
  QueryCommand,
  PutCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

import BladeDocument from "./BladeDocument";
import {
  ValueFilter,
  CollectionSchema,
  Option,
  CollectionName,
  CollectionSchemaKey,
} from "./BladeType";
import { decodeNext, buildItems, encodeNext, buildItem } from "./utils/index";
import BladeFilter from "./BladeFilter";

export default class BladeCollection<
  Opt extends Option,
  Collection extends CollectionName<Opt>
> {
  private option: Opt;
  private collection: Collection;

  constructor(option: Opt, collection: Collection) {
    this.option = option;
    this.collection = collection;
  }

  is<K extends CollectionSchemaKey<Opt, Collection>>(key: K) {
    return new BladeDocument<Opt, Collection>(
      this.option,
      this.collection,
      key
    );
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
      return buildItems<Opt, Collection>(
        result.Items,
        encodeNext(result.LastEvaluatedKey),
        this.option
      );
    } catch (err) {
      console.warn(
        `Failed to get ${getFieldValue("PRIMARY_KEY")} (${err.message})`
      );
      return buildItems<Opt, Collection>([], null, this.option);
    }
  }

  addLater(key: string, value: CollectionSchema<Opt, Collection>) {
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

  async add(key: string, value: CollectionSchema<Opt, Collection>) {
    const command = this.addLater(key, value);
    try {
      const result = await this.option.client.send(command);
      if (result.$metadata.httpStatusCode === 200) {
        return buildItem<Opt, Collection>(command.input.Item, this.option);
      }
    } catch (err) {
      console.warn(
        `Failed to add ${this.option.getFieldValue("PRIMARY_KEY")} (${
          err.message
        })`
      );
    }
  }

  where<F extends keyof CollectionSchema<Opt, Collection>>(
    field: F,
    condition: ValueFilter,
    value:
      | CollectionSchema<Opt, Collection>[F]
      | Array<CollectionSchema<Opt, Collection>[F]>
  ) {
    return new BladeFilter<Opt, Collection>(
      this.option,
      field,
      condition,
      Array.isArray(value) ? value : [value]
    );
  }
}
