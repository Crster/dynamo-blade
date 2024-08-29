import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import {
  ValueFilter,
  CollectionSchema,
  Option,
  CollectionName,
  CollectionSchemaKey,
} from "./BladeType";
import { buildItems, decodeNext } from "./utils";

export default class BladeFilter<
  Opt extends Option,
  Collection extends string & CollectionName<Opt>
> {
  private option: Opt;
  private collection: Collection;
  private key: Partial<CollectionSchemaKey<Opt, Collection>>;
  private tailed: boolean;
  private filters: Array<[any, ValueFilter, ...any]>;

  constructor(
    option: Opt,
    collection: Collection,
    key: Partial<CollectionSchemaKey<Opt, Collection>>,
    tailed: boolean,
    field: any,
    condition: ValueFilter,
    value: Array<any>
  ) {
    this.option = option;
    this.collection = collection;
    this.key = key;
    this.tailed = tailed;
    this.filters = [[field, condition, value]];
  }

  where<F extends keyof CollectionSchema<Opt, Collection>>(
    field: F,
    condition: ValueFilter,
    value:
      | CollectionSchema<Opt, Collection>[F]
      | Array<CollectionSchema<Opt, Collection>[F]>
  ) {
    this.filters.push([
      field,
      condition,
      Array.isArray(value) ? value : [value],
    ]);
    return this;
  }

  async get(next?: string) {
    const { client, tableName, primaryKey, schema } = this.option;

    const filterExpression: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    // Build Key Condition
    const sortKey = primaryKey.sortKey ? primaryKey.sortKey[0] : undefined;
    const key = schema[this.collection].getKey(this.option, this.key);

    keyConditionExpression.push(`${primaryKey.hashKey[0]} = :hashKey`);
    expressionAttributeValues.set(":hashKey", key[primaryKey.hashKey[0]]);

    let counter = 0;
    for (const [field, condition, value] of this.filters) {
      counter++;

      // Build Filter Condition
      if (field !== sortKey) {
        keyConditionExpression.push(
          `begins_with(${sortKey}, :sortKey${counter})`
        );
        expressionAttributeValues.set(`:sortKey${counter}`, this.collection);
      }

      const filterCondition =
        field === sortKey ? keyConditionExpression : filterExpression;

      switch (condition) {
        case "=":
          filterCondition.push(`#field${counter} = :value${counter}`);
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:value${counter}`, value.at(0));
          break;
        case "!=":
          filterCondition.push(`#field${counter} <> :value${counter}`);
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:value${counter}`, value.at(0));
          break;
        case "<":
          filterCondition.push(`#field${counter} < :value${counter}`);
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:value${counter}`, value.at(0));
          break;
        case "<=":
          filterCondition.push(`#field${counter} <= :value${counter}`);
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:value${counter}`, value.at(0));
          break;
        case ">":
          filterCondition.push(`#field${counter} > :value${counter}`);
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:value${counter}`, value.at(0));
          break;
        case ">=":
          filterCondition.push(`#field${counter} >= :value${counter}`);
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:value${counter}`, value.at(0));
          break;
        case "BETWEEN":
          filterCondition.push(
            `#field${counter} BETWEEN :valueFrom${counter} AND :valueTo${counter}`
          );
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:valueFrom${counter}`, value.at(0));
          expressionAttributeValues.set(`:valueTo${counter}`, value.at(1));
          break;
        case "IN":
          const values: Array<any> = [];
          value.forEach((val, index) => {
            values.push(`:value${index}`);
            expressionAttributeValues.set(`:value${index}`, val);
          });

          filterCondition.push(`#field${counter} IN (${values.join(",")})`);
          expressionAttributeNames.set(`#field${counter}`, field);
          break;
        case "BEGINS_WITH":
          filterCondition.push(
            `begins_with(#field${counter}, :value${counter})`
          );
          expressionAttributeNames.set(`#field${counter}`, field);
          expressionAttributeValues.set(`:value${counter}`, value.at(0));
          break;
      }
    }

    const command = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: keyConditionExpression.join(" AND "),
      FilterExpression:
        filterExpression.length > 0
          ? filterExpression.join(" AND ")
          : undefined,
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
      ExclusiveStartKey: decodeNext(next),
      ScanIndexForward: !this.tailed,
    });

    const result = await client.send(command);
    return buildItems<Opt, Collection>(this.option, this.collection, result);
  }
}
