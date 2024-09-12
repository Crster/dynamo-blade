import { ScanCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { BladeItem, BladeViewField } from "./BladeType";
import {
  BladeSchema,
  BladeViewCondition,
  DynamoBladeOption,
  BladeSchemaKey,
  BladeResult,
} from "./BladeType";
import {
  decodeNext,
  encodeNext,
  fillMap,
  generateItem,
  generateValue,
  getCondition,
  getKey,
} from "./BladeUtility";

type QueryType = "QUERY" | "QUERYDESC" | "SCAN";
type Conjunctions = "AND" | "OR";

export default class BladeView<Schema extends BladeSchema> {
  private readonly option: DynamoBladeOption;
  private readonly schema: Schema;
  private readonly key: BladeSchemaKey<Schema>;
  private readonly index: string;
  private readonly type: QueryType;
  private readonly conditions: Array<{
    field: string;
    condition: BladeViewCondition;
    value: any;
    conjunction: Conjunctions;
  }>;

  constructor(
    option: DynamoBladeOption,
    schema: Schema,
    key: BladeSchemaKey<Schema>,
    type: QueryType,
    index?: string
  ) {
    this.option = option;
    this.schema = schema;
    this.key = key;
    this.type = type;
    this.index = index;

    this.conditions = [];
  }

  where(field: BladeViewField<Schema>, condition: BladeViewCondition, value: any) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  and(field: BladeViewField<Schema>, condition: BladeViewCondition, value: any) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  or(field: BladeViewField<Schema>, condition: BladeViewCondition, value: any) {
    this.conditions.push({ field, condition, value, conjunction: "OR" });
    return this;
  }

  async get(next?: string) {
    if (this.type === "QUERY") {
      return this._getByQuery(true, next);
    } else if (this.type === "QUERYDESC") {
      return this._getByQuery(false, next);
    } else {
      return this._getByScan(next);
    }
  }

  private _generateValue(field: string, value: any) {
    if (this.option.schema.hashKey.field === field) {
      return generateValue(
        "SET",
        this.option.schema.hashKey.type,
        this.key.hashKey(value)
      );
    } else if (this.option.schema.sortKey?.field === field) {
      return generateValue(
        "SET",
        this.option.schema.sortKey.type,
        this.key.sortKey(value)
      );
    } else {
      const attribute = this.schema[field];
      if (typeof attribute["value"] === "function") {
        return attribute["value"]("SET", value);
      } else {
        return value;
      }
    }
  }

  private async _getByQuery(forwardScan: boolean, next?: string) {
    const ret: BladeResult<BladeItem<Schema>> = { items: [] };
    let counter = 0;

    const filterExpressionAnd: Array<string> = [];
    const filterExpressionOr: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    const key = getKey(this.option, this.index);
    if (key.hashKey) {
      const hashCondition = this.conditions.find(
        (ii) => ii.field === "HASH" && ii.conjunction === "AND"
      );
      if (hashCondition) {
        const condition = getCondition(
          hashCondition.field,
          hashCondition.condition,
          this._generateValue(key.hashKey.field, hashCondition.value),
          counter
        );
        keyConditionExpression.push(...condition.filter);
        fillMap(expressionAttributeNames, condition.field);
        fillMap(expressionAttributeValues, condition.values);

        counter++;
      }
    }
    if (key.sortKey) {
      const sortCondition = this.conditions.find(
        (ii) => ii.field === "SORT" && ii.conjunction === "AND"
      );
      if (sortCondition) {
        const condition = getCondition(
          sortCondition.field,
          sortCondition.condition,
          this._generateValue(key.sortKey.field, sortCondition.value),
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
      if (condi.field === "HASH" && condi.conjunction === "AND")
        continue;
      if (condi.field === "SORT" && condi.conjunction === "AND")
        continue;

      const condition = getCondition(
        condi.field,
        condi.condition,
        this._generateValue(condi.field, condi.value),
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

    const command = new QueryCommand({
      TableName: this.option.table,
      IndexName: this.index,
      ScanIndexForward: forwardScan,
      FilterExpression: filterExpression,
      ExclusiveStartKey: decodeNext(next),
      KeyConditionExpression: keyConditionExpression.join(" AND "),
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
    });

    const result = await this.option.client.send(command);

    for (const ii of result.Items) {
      ret.items.push(generateItem(this.schema, ii, "GET"));
    }

    if (result.LastEvaluatedKey) {
      ret.next = encodeNext(result.LastEvaluatedKey);
    }

    return ret;
  }

  private async _getByScan(next?: string) {
    const ret: BladeResult<BladeItem<Schema>> = { items: [] };
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
        this._generateValue(condi.field, condi.value),
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
      TableName: this.option.table,
      IndexName: this.index,
      ExclusiveStartKey: decodeNext(next),
      FilterExpression: filterExpression,
      ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
      ExpressionAttributeValues: Object.fromEntries(expressionAttributeValues),
    });

    const result = await this.option.client.send(command);

    for (const ii of result.Items) {
      ret.items.push(generateItem(this.schema, ii, "GET"));
    }

    if (result.LastEvaluatedKey) {
      ret.next = encodeNext(result.LastEvaluatedKey);
    }

    return ret;
  }
}
