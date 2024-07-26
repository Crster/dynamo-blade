import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import BladeOption from "./BladeOption";
import { SimpleFilter } from "./BladeType";
import { buildItems, decodeNext, encodeNext } from "./utils";

export default class BladeFilter<Schema> {
  private option: BladeOption;
  private filters: Array<[any, SimpleFilter, ...any]>;

  constructor(
    option: BladeOption,
    field: any,
    condition: SimpleFilter,
    value: Array<any>
  ) {
    this.option = option;
    this.filters = [[field, condition, value]];
  }

  where<F extends keyof Schema>(
    field: F,
    condition: SimpleFilter,
    ...value: Array<Schema[F]>
  ) {
    this.filters.push([field, condition, value]);
    return this;
  }

  async get(next?: string) {
    const {
      client,
      tableName,
      collection,
      isUseIndex,
      getFieldName,
      getFieldValue,
    } = this.option;

    const filterExpression: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

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
    expressionAttributeValues.set(":hashKey", hashKeyValue as any);

    let counter = 0;
    for (const [field, condition, value] of this.filters) {
      console.log(value)
      counter++;

      // Build Filter Condition
      if (field !== sortKey) {
        if (!isUseIndex()) {
          keyConditionExpression.push(
            `begins_with(${sortKey}, :sortKey${counter})`
          );
          expressionAttributeValues.set(
            `:sortKey${counter}`,
            collection as any
          );
        }
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

    try {
      const command = new QueryCommand({
        TableName: tableName,
        IndexName: isUseIndex() ? getFieldName("INDEX") : undefined,
        KeyConditionExpression: keyConditionExpression.join(" AND "),
        FilterExpression:
          filterExpression.length > 0
            ? filterExpression.join(" AND ")
            : undefined,
        ExpressionAttributeValues: Object.fromEntries(
          expressionAttributeValues
        ),
        ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
        ExclusiveStartKey: decodeNext(next),
        ScanIndexForward: this.option.forwardScan,
      });

      const result = await client.send(command);
      return buildItems<Schema>(
        result.Items,
        encodeNext(result.LastEvaluatedKey),
        this.option
      );
    } catch (err) {
      console.warn(`Failed to get ${collection} (${err.message})`);
      return buildItems<Schema>([], null, this.option);
    }
  }
}
