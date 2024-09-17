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
  fromDbValue,
  getCondition,
  getDbKey,
  getPrimaryKeyField,
} from "./BladeUtility";

type QueryType = "QUERY" | "SCAN";
type Conjunctions = "AND" | "OR";

export class BladeView<
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
  private primaryKeyField: string;

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
    field: string & keyof BladeItem<Type["type"]>,
    condition: ValueFilter | DataFilter,
    value: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  and(
    field: string & keyof BladeItem<Type["type"]>,
    condition: ValueFilter | DataFilter,
    value: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  or(
    field: string & keyof BladeItem<Type["type"]>,
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
    const pk = primaryKeyValue ?? "";

    if (Array.isArray(value)) {
      return value.map((ii) => pk + String(ii ?? ""));
    } else {
      return pk + String(value ?? "");
    }
  }

  private async _getByQuery(next?: string) {
    const ret: BladeResult<BladeItem<Type["type"]>> = { items: [] };
    let counter = 0;

    let hashKey = this.option.schema.table.hashKey;
    let sortKey = this.option.schema.table.sortKey;

    const filterExpressionAnd: Array<string> = [];
    const filterExpressionOr: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    if (this.index) {
      hashKey = undefined;
      sortKey = undefined;

      const indexSchema = this.option.schema.index[this.index];
      if (indexSchema) {
        if (indexSchema.type === "LOCAL") {
          hashKey = this.option.schema.table.hashKey;
        } else {
          if (indexSchema.hashKey) {
            hashKey = indexSchema.hashKey[0];
          }
        }
        if (indexSchema.sortKey) {
          sortKey = indexSchema.sortKey[0];
        }
      }

      if (hashKey && !sortKey) {
        this.primaryKeyField = hashKey;
      } else if (sortKey) {
        this.primaryKeyField = sortKey;
      }
    }

    const primaryCondition = this.conditions.find(
      (ii) => ii.field === this.primaryKeyField && ii.conjunction === "AND"
    );

    let dbKey = this.index
      ? this.conditions.reduce((p, c) => {
          p[c.field] = c.value;
          return p;
        }, {})
      : getDbKey(this.option.schema, this.key, this.index);
    if (hashKey && sortKey) {
      const condition = getCondition(
        hashKey,
        "=",
        this.key.length > 2
          ? dbKey[hashKey]
          : this._generatePrimaryKeyValue(
              primaryCondition?.value,
              dbKey[hashKey]
            ),
        counter
      );
      keyConditionExpression.push(...condition.filter);
      fillMap(expressionAttributeNames, condition.field);
      fillMap(expressionAttributeValues, condition.values);

      counter++;

      if (primaryCondition) {
        const condition = getCondition(
          sortKey,
          primaryCondition.condition,
          this._generatePrimaryKeyValue(primaryCondition.value, dbKey[sortKey]),
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
          sortKey,
          primaryCondition.condition,
          this._generatePrimaryKeyValue(primaryCondition.value, dbKey[sortKey]),
          counter
        );
        keyConditionExpression.push(...condition.filter);
        fillMap(expressionAttributeNames, condition.field);
        fillMap(expressionAttributeValues, condition.values);

        counter++;
      } else {
        const condition = getCondition(hashKey, "=", dbKey[hashKey], counter);
        keyConditionExpression.push(...condition.filter);
        fillMap(expressionAttributeNames, condition.field);
        fillMap(expressionAttributeValues, condition.values);

        counter++;
      }
    }

    let filterExpression: string;
    for (const condi of this.conditions) {
      if (condi === primaryCondition) continue;
      if (condi.field === hashKey) continue;
      if (condi.field === sortKey) continue;

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
      ret.items.push(fromDbValue(this.option.schema, this.key, ii));
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
      ret.items.push(fromDbValue(this.option.schema, this.key, ii));
    }

    if (result.LastEvaluatedKey) {
      ret.next = encodeNext(result.LastEvaluatedKey);
    }

    return ret;
  }
}
