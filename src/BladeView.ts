import { QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import {
  BladeItem,
  BladeResult,
  BladeType,
  DataFilter,
  ValueFilter,
} from "./BladeType";
import { decodeNext, encodeNext, fillMap, getCondition } from "./BladeUtility";
import { BladeKeySchema } from "./BladeKeySchema";

export class BladeView<Type extends BladeType<any>> {
  private readonly blade: BladeKeySchema<any>;
  private readonly conditions: Array<{
    field: string;
    condition: ValueFilter | DataFilter;
    value: any;
    conjunction: "AND" | "OR";
  }>;

  constructor(blade: BladeKeySchema<any>) {
    this.blade = blade;
    this.conditions = [];
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
    const ret: BladeResult<BladeItem<Type["type"]>> = { items: [] };

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
