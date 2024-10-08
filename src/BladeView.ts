import { QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import {
  BladeAttribute,
  BladeAttributeSchema,
  BladeItem,
} from "./BladeAttribute";
import { Blade } from "./Blade";
import {
  decodeNext,
  encodeNext,
  fillMap,
  getCondition,
  getSchemaFromTypeKey,
  RecordOfBladeItemList,
} from "./BladeUtility";

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
  count: number;
  data: Type;
  next?: string;
}

export type ArrayResult<T extends BladeAttribute<BladeAttributeSchema>> =
  BladeResult<Array<BladeItem<T>>>;

export type RecordResult<
  T extends Record<string, BladeAttribute<BladeAttributeSchema>>
> = BladeResult<RecordOfBladeItemList<T>>;

export class BladeView<Attribute, Result extends BladeResult<any>> {
  private readonly blade: Blade<any>;
  private readonly conditions: Array<{
    field: string;
    condition: KeyFilter | DataFilter;
    value: any;
    conjunction: "AND" | "OR";
  }>;
  private readonly result: Result;

  constructor(blade: Blade<any>, result: Result) {
    this.blade = blade;
    this.conditions = [];
    this.result = result;
  }

  and(
    field: string & keyof Attribute,
    condition: KeyFilter | DataFilter,
    value?: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "AND" });
    return this;
  }

  or(
    field: string & keyof Attribute,
    condition: KeyFilter | DataFilter,
    value?: any
  ) {
    this.conditions.push({ field, condition, value, conjunction: "OR" });
    return this;
  }

  condition(next?: string) {
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

    return command;
  }

  async get(next?: string) {
    const ret: Result = JSON.parse(JSON.stringify(this.result));

    const command = this.condition(next);
    const result = await this.blade.execute(command);

    if (result["Items"]) {
      const typeKey = this.blade.getKeyField("TypeKey").at(0);
      ret.count = result["Count"];

      const groups = new Map<string, BladeAttribute<BladeAttributeSchema>>();
      for (const ii of result["Items"]) {
        const group: string = ii[typeKey.field];

        if (!groups.has(group)) {
          groups.set(group, getSchemaFromTypeKey(this.blade.table, group));
        }

        if (Array.isArray(ret.data)) {
          ret.data.push(this.blade.buildItem(ii, groups.get(group)));
        } else {
          if (!ret.data[group]) {
            ret.data[group] = [];
          }

          ret.data[group].push(this.blade.buildItem(ii, groups.get(group)));
        }
      }
    }

    if (result["LastEvaluatedKey"]) {
      ret.next = encodeNext(result["LastEvaluatedKey"]);
    }

    return ret;
  }

  async find(limit?: number) {
    const ret: Result = JSON.parse(JSON.stringify(this.result));

    let next: string;
    do {
      const result = await this.get(next);

      if (result.count) {
        if (Array.isArray(result.data)) {
          ret.data.push(...result.data);
        } else {
          for (const k in result.data) {
            if (!ret.data[k]) {
              ret.data[k] = [];
            }

            ret.data[k].push(...result.data[k]);
          }
        }

        ret.count += result.count;
        if (limit && ret.count >= limit) {
          break;
        }

        next = result.next;
      }
    } while (next);

    return ret;
  }
}
