import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import BladeSchema from "./BladeSchema";

export type Option = {
  tableName: string;
  client: DynamoDBDocumentClient;
  schema: Record<string, ReturnType<typeof BladeSchema<any>>>;
};

export type CollectionName<Opt extends Option> = keyof Opt["schema"];

export type BladeItem<
  Opt extends Option,
  Collection extends keyof Opt["schema"]
> = Opt["schema"][Collection];

export type FieldType =
  | "HASH"
  | "SORT"
  | "HASH_INDEX"
  | "SORT_INDEX"
  | "PRIMARY_KEY"
  | "INDEX";

export type BillingMode = "PROVISIONED" | "PAY_PER_REQUEST";

export type ValueFilter =
  | "="
  | "!="
  | ">"
  | ">="
  | "<"
  | "<="
  | "BETWEEN"
  | "BEGINS_WITH"
  | "IN";

export type DataFilter =
  | "ATTRIBUTE_EXISTS"
  | "ATTRIBUTE_NOT_EXISTS"
  | "ATTRIBUTE_TYPE"
  | "CONTAINS"
  | "SIZE"
  | "SIZE_GT"
  | "SIZE_LT";

export type QueryResult<T> = { items: Array<T>; next?: string };

export type RemoveField<T> = {
  [P in keyof T]?: boolean;
};

export type UpdateValue<T> =
  | Partial<T>
  | { $add: Partial<T> }
  | { $set: Partial<T> }
  | { $remove: RemoveField<T> }
  | { $delete: Partial<T> };

export type Condition = {
  field: string;
  condition: ValueFilter | DataFilter;
  value?: any;
};
