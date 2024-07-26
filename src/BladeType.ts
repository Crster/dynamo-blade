import { DynamoDBClient } from "@aws-sdk/client-dynamodb/dist-types/DynamoDBClient";

export type Option = {
  tableName: string;
  client: DynamoDBClient;
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

export type Entity<T> = Pick<T, ReadOnlyField<T>>;

export interface KeyField {
  PK: string;
  SK: string;
  GS1PK: string;
  GS1SK: string;
}

export type EntityField<T> = Pick<T, WritableField<T>> & KeyField;

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
  field: "ANY" | T;
  condition: Condition;
  value?: any;
};
