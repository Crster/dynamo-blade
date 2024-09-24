import { QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import {
  BladeAttribute,
  BladeAttributeSchema,
  BladeItem,
} from "./BladeAttribute";
import { decodeNext, encodeNext, fillMap, getCondition } from "./BladeUtility";
import { Blade } from "./Blade";

export type KeyFilter =
  | "="
  | "!="
  | ">"
  | ">="
  | "<"
  | "<="
  | "BETWEEN"
  | "BEGINS_WITH";

export type DataFilter =
  | "IN"
  | "ATTRIBUTE_EXISTS"
  | "ATTRIBUTE_NOT_EXISTS"
  | "ATTRIBUTE_TYPE"
  | "CONTAINS"
  | "SIZE"
  | "SIZE_GT"
  | "SIZE_LT";

export interface BladeResult<Type> {
  items: Array<Type>;
  next?: string;
}

export class BladeView<Attribute extends BladeAttribute<BladeAttributeSchema>> {
  private readonly blade: Blade<any>;
  private readonly conditions: Array<{
    field: string;
    condition: KeyFilter | DataFilter;
    value: any;
    conjunction: "AND" | "OR";
  }>;

  constructor(blade: Blade<any>) {
    this.blade = blade;
    this.conditions = [];
  }

  and(
    field: string & keyof BladeItem<Attribute>,
    condition: KeyFilter | DataFilter,
    value: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  or(
    field: string & keyof BladeItem<Attribute>,
    condition: KeyFilter | DataFilter,
    value: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "OR" });
    return this;
  }

  async get(next?: string) {
    const ret: BladeResult<BladeItem<Attribute>> = { items: [] };

    let counter: number = 0;
    let filterExpression: string;

    const filterExpressionAnd: Array<string> = [];
    const filterExpressionOr: Array<string> = [];
    const keyConditionExpression: Array<string> = [];
    const expressionAttributeNames = new Map<string, string>();
    const expressionAttributeValues = new Map<string, any>();

    if (this.blade.hasKey()) {
      for (const key of this.blade.getKeyCondition()) {
        const condition = getCondition(
          key.primaryKey,
          key.condition,
          key.value,
          counter
        );

        keyConditionExpression.push(...condition.filter);
        fillMap(expressionAttributeNames, condition.field);
        fillMap(expressionAttributeValues, condition.values);
        counter++;
      }
    }

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

    let command: QueryCommand | ScanCommand;
    if (this.blade.hasKey()) {
      command = new QueryCommand({
        TableName: this.blade.getTableName(),
        IndexName: this.blade.getIndex(),
        ScanIndexForward: this.blade.getScanIndexForward(),
        FilterExpression: filterExpression,
        ExclusiveStartKey: decodeNext(next),
        KeyConditionExpression: keyConditionExpression.join(" AND "),
        ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
        ExpressionAttributeValues: Object.fromEntries(
          expressionAttributeValues
        ),
      });
    } else {
      command = new ScanCommand({
        TableName: this.blade.getTableName(),
        IndexName: this.blade.getIndex(),
        ExclusiveStartKey: decodeNext(next),
        FilterExpression: filterExpression,
        ExpressionAttributeNames: Object.fromEntries(expressionAttributeNames),
        ExpressionAttributeValues: Object.fromEntries(
          expressionAttributeValues
        ),
      });
    }

    const result = await this.blade.execute(command);

    for (const ii of result["Items"]) {
      ret.items.push(ii);
    }

    if (result["LastEvaluatedKey"]) {
      ret.next = encodeNext(result["LastEvaluatedKey"]);
    }

    return ret;
  }
}
