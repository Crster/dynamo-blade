import { QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import {
  BladeItem,
  BladeOption,
  BladeResult,
  BladeSchema,
  BladeType,
  DataFilter,
  ValueFilter,
} from "./BladeType";
import {
  decodeNext,
  encodeNext,
  fillMap,
  getCondition,
  getDbKey,
  getDbValue,
  getPrimaryKeyField,
} from "./BladeUtility";

type QueryType = "QUERY" | "SCAN";
type Conjunctions = "AND" | "OR";

export default class BladeView<
  Schema extends BladeSchema,
  Type extends BladeType<any>
> {
  private readonly option: BladeOption<Schema>;
  private readonly key: Array<string>;
  private readonly queryType: QueryType;
  private readonly index: string;
  private readonly conditions: Array<{
    field: string;
    condition: ValueFilter | DataFilter;
    value: any;
    conjunction: Conjunctions;
  }>;
  private readonly primaryKeyField: string;

  constructor(
    option: BladeOption<Schema>,
    key: Array<string>,
    type: QueryType,
    index?: string
  ) {
    this.option = option;
    this.key = key;
    this.key = key;
    this.queryType = type;
    this.index = index;
    this.primaryKeyField = getPrimaryKeyField(this.option.schema, this.key);

    this.conditions = [];
  }

  where(
    field: string & keyof Type["type"],
    condition: ValueFilter | DataFilter,
    value: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  and(
    field: string & keyof Type["type"],
    condition: ValueFilter | DataFilter,
    value: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  or(
    field: string & keyof Type["type"],
    condition: ValueFilter | DataFilter,
    value: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "OR" });
    return this;
  }

  async get(next?: string) {
    if (this.queryType === "QUERY") {
      return this._getByQuery(next);
    } else {
      return this._getByScan(next);
    }
  }

  private _generatePrimaryKeyValue(value: any, primaryKeyValue: string) {
    if (Array.isArray(value)) {
      return value.map((ii) => primaryKeyValue + String(ii));
    } else {
      return primaryKeyValue + String(value);
    }
  }

  private async _getByQuery(next?: string) {
    const ret: BladeResult<BladeItem<Type["type"]>> = { items: [] };
    let counter = 0;

    const filterExpressionAnd: Array<string> = [];
    const filterExpressionOr: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    const primaryCondition = this.conditions.find(
      (ii) => ii.field === this.primaryKeyField && ii.conjunction === "AND"
    );

    let dbKey = getDbKey(this.option.schema, this.key);
    if (this.option.schema.table.hashKey && this.option.schema.table.sortKey) {
      const condition = getCondition(
        this.option.schema.table.hashKey,
        "=",
        dbKey[this.option.schema.table.hashKey],
        counter
      );
      keyConditionExpression.push(...condition.filter);
      fillMap(expressionAttributeNames, condition.field);
      fillMap(expressionAttributeValues, condition.values);

      counter++;

      if (primaryCondition) {
        const condition = getCondition(
          this.option.schema.table.sortKey,
          primaryCondition.condition,
          this._generatePrimaryKeyValue(
            primaryCondition.value,
            dbKey[this.option.schema.table.sortKey]
          ),
          counter
        );
        keyConditionExpression.push(...condition.filter);
        fillMap(expressionAttributeNames, condition.field);
        fillMap(expressionAttributeValues, condition.values);

        counter++;
      }
    } else {
      if (primaryCondition) {
        const condition = getCondition(
          this.option.schema.table.sortKey,
          primaryCondition.condition,
          this._generatePrimaryKeyValue(
            primaryCondition.value,
            dbKey[this.option.schema.table.sortKey]
          ),
          counter
        );
        keyConditionExpression.push(...condition.filter);
        fillMap(expressionAttributeNames, condition.field);
        fillMap(expressionAttributeValues, condition.values);

        counter++;
      } else {
        const condition = getCondition(
          this.option.schema.table.hashKey,
          "=",
          dbKey[this.option.schema.table.hashKey],
          counter
        );
        keyConditionExpression.push(...condition.filter);
        fillMap(expressionAttributeNames, condition.field);
        fillMap(expressionAttributeValues, condition.values);

        counter++;
      }
    }

    let filterExpression: string;
    for (const condi of this.conditions) {
      if (condi === primaryCondition) continue;

      const condition = getCondition(
        condi.field,
        condi.condition,
        condi.value,
        counter
      );
      if (condi.conjunction === "AND") {
        filterExpressionAnd.push(...condition.filter);
      } else if (condi.conjunction === "OR") {
        filterExpressionOr.push(...condition.filter);
      }

      fillMap(expressionAttributeNames, condition.field);
      fillMap(expressionAttributeValues, condition.values);

      counter++;
    }

    if (filterExpressionAnd.length) {
      filterExpression = filterExpressionAnd.join(" AND ");
    }

    if (filterExpressionOr.length) {
      if (filterExpression) {
        filterExpression += "AND " + filterExpressionOr.join(" OR ");
      } else {
        filterExpression = filterExpressionOr.join(" OR ");
      }
    }

    const scanIndexForward =
      primaryCondition &&
      [">", "<", ">=", "<=", "BETWEEN"].includes(primaryCondition.condition)
        ? false
        : true;

    const command = new QueryCommand({
      TableName: this.option.schema.table.name,
      IndexName: this.index,
      ScanIndexForward: scanIndexForward,
      FilterExpression: filterExpression,
      ExclusiveStartKey: decodeNext(next),
      KeyConditionExpression: keyConditionExpression.join(" AND "),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
    });

    const result = await this.option.client.send(command);

    for (const ii of result.Items) {
      ret.items.push(getDbValue(this.option.schema, this.key, ii));
    }

    if (result.LastEvaluatedKey) {
      ret.next = encodeNext(result.LastEvaluatedKey);
    }

    return ret;
  }

  private async _getByScan(next?: string) {
    const ret: BladeResult<BladeItem<Type["type"]>> = { items: [] };
    let counter = 0;

    const filterExpressionAnd: Array<string> = [];
    const filterExpressionOr: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    let filterExpression: string;
    for (const condi of this.conditions) {
      const condition = getCondition(
        condi.field,
        condi.condition,
        condi.value,
        counter
      );
      if (condi.conjunction === "AND") {
        filterExpressionAnd.push(...condition.filter);
      } else if (condi.conjunction === "OR") {
        filterExpressionOr.push(...condition.filter);
      }

      fillMap(expressionAttributeNames, condition.field);
      fillMap(expressionAttributeValues, condition.values);

      counter++;
    }

    if (filterExpressionAnd.length) {
      filterExpression = filterExpressionAnd.join(" AND ");
    }

    if (filterExpressionOr.length) {
      if (filterExpression) {
        filterExpression += "AND " + filterExpressionOr.join(" OR ");
      } else {
        filterExpression = filterExpressionOr.join(" OR ");
      }
    }

    const command = new ScanCommand({
      TableName: this.option.schema.table.name,
      IndexName: this.index,
      ExclusiveStartKey: decodeNext(next),
      FilterExpression: filterExpression,
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
    });

    const result = await this.option.client.send(command);

    for (const ii of result.Items) {
      ret.items.push(getDbValue(this.option.schema, this.key, ii));
    }

    if (result.LastEvaluatedKey) {
      ret.next = encodeNext(result.LastEvaluatedKey);
    }

    return ret;
  }
}
