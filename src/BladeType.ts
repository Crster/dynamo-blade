import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

export type Option = {
  tableName: string;
  client: DynamoDBDocumentClient;
};

type IfEquals<X, Y, A = X, B = never> = (<T>() => T extends X ? 1 : 2) extends <
  T
>() => T extends Y ? 1 : 2
  ? A
  : B;

type WritableField<T> = {
  [P in keyof T]-?: IfEquals<
    { [Q in P]: T[P] },
    { -readonly [Q in P]: T[P] },
    P
  >;
}[keyof T];

type ReadOnlyField<T> = {
  [P in keyof T]-?: IfEquals<
    { [Q in P]: T[P] },
    { -readonly [Q in P]: T[P] },
    never,
    P
  >;
}[keyof T];

export type CollectionName<T> = keyof Pick<T, ReadOnlyField<T>>;

export type Item<T> = Pick<T, WritableField<T>>;

export type ItemSchema<T> = Pick<T, WritableField<T>> & {
  PK: string;
  SK: string;
  GS1PK: string;
  GS1SK: string;
};

export type BladeItem<T> = Pick<T, WritableField<T>> & { PK: string; SK?: string }

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
