import { DynamoDBClient } from "@aws-sdk/client-dynamodb/dist-types/DynamoDBClient";

export type Option = {
  tableName: string;
  client: DynamoDBClient;
  hashKey?: string;
  sortKey?: string;
  indexName?: string;
  separator?: string;
};

export type Model<T> = keyof T;

export type FieldType =
  | "HASH"
  | "SORT"
  | "HASH_INDEX"
  | "SORT_INDEX"
  | "PRIMARY_KEY"
  | "INDEX";

export type BillingMode = "PROVISIONED" | "PAY_PER_REQUEST";

export type SimpleFilter =
  | "="
  | "!="
  | ">"
  | ">="
  | "<"
  | "<="
  | "BETWEEN"
  | "BEGINS_WITH"
  | "IN";

export type ExtraFilter =
  | "ATTRIBUTE_EXISTS"
  | "ATTRIBUTE_NOT_EXISTS"
  | "ATTRIBUTE_TYPE"
  | "CONTAINS"
  | "SIZE"
  | "SIZE_GT"
  | "SIZE_LT";

export type Condition = SimpleFilter | ExtraFilter;

export type QueryResult<T> = { items: Array<T>; next?: string };

export type RemoveValue<T> = {
  [P in keyof T]?: boolean;
};

export type UpdateValue<T> =
  | Partial<T>
  | { $add: Partial<T> }
  | { $set: Partial<T> }
  | { $remove: RemoveValue<T> }
  | { $delete: Partial<T> };

export type ConditionDefination<T> = {
  field: T;
  condition: Condition;
  value?: any;
};
