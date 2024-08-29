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
  BladeItem
} from "./BladeType";
import { buildItems, decodeNext } from "./utils/index";
import BladeFilter from "./BladeFilter";

export default class BladeCollection<
  Opt extends Option,
  Collection extends string & CollectionName<Opt>
> {
  private option: Opt;
  private collection: Collection;
  private key: Partial<CollectionSchemaKey<Opt, Collection>>;
  private tailed: boolean;

  constructor(option: Opt, collection: Collection) {
    this.option = option;
    this.collection = collection;
  }

  is<K extends Partial<CollectionSchemaKey<Opt, Collection>>>(
    hashKey: K,
    sortKey?: K
  ) {
    if (sortKey) {
      return new BladeDocument<Opt, Collection>(this.option, this.collection, {
        ...hashKey,
        ...sortKey,
      });
    } else {
      this.key = hashKey;
      return this;
    }
  }

  tail() {
    this.tailed = true;
    return this;
  }

  async get(next?: string) {
    const { client, tableName, primaryKey, schema } = this.option;

    if (!this.key)
      throw new Error("Must call is(hashKey) before calling get()");

    const key = schema[this.collection].getKey(this.option, this.key);

    const input: QueryCommandInput = {
      TableName: tableName,
      KeyConditionExpression: `${primaryKey.hashKey[0]} = :hashKey`,
      ExpressionAttributeValues: {
        ":hashKey": key[primaryKey.hashKey[0]],
      },
      ExclusiveStartKey: decodeNext(next),
      ScanIndexForward: !this.tailed,
    };

    const result = await client.send(new QueryCommand(input));
    return buildItems<Opt, Collection>(this.option, this.collection, result);
  }

  addLater(value: CollectionSchema<Opt, Collection>) {
    const { tableName, schema } = this.option;

    const val = schema[this.collection].getItem(value);
    const key = schema[this.collection].getKey(this.option, value);

    const command = new PutCommand({
      TableName: tableName,
      Item: {
        ...val,
        ...key,
      },
    });

    return command;
  }

  async add(value: Partial<BladeItem<Opt["schema"][Collection]>>) {
    const command = this.addLater(value);
    const result = await this.option.client.send(command);
    if (result.$metadata.httpStatusCode === 200) {
      return this.option.schema[this.collection].getItem<BladeItem<Opt["schema"][Collection]>>(value);
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
      this.collection,
      this.key,
      this.tailed,
      field,
      condition,
      Array.isArray(value) ? value : [value]
    );
  }
}
