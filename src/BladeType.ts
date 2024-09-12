import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

export type BladeOperation = "ADD" | "GET" | "SET";

export type BladeItemType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | BufferConstructor
  | DateConstructor;

export interface BladeKey {
  field: string;
  type: BladeItemType;
}

export interface DynamoBladeIndex {
  hashKey?: BladeKey;
  sortKey?: BladeKey;
  type: "LOCAL" | "GLOBAL";
  projection?: "ALL" | "KEYS" | Array<string>;
  provision?: { read: number; write: number };
}

export interface BladeSchemaDefinition {
  hashKey: BladeKey;
  sortKey?: BladeKey;
  createdOn?: boolean | string;
  modifiedOn?: boolean | string;
  index?: Record<string, DynamoBladeIndex>;
}

export interface DynamoBladeOption {
  table: string;
  client: DynamoDBDocumentClient;
  schema: BladeSchemaDefinition;
}

export type BladeCollectionType =
  | ArrayConstructor
  | SetConstructor
  | MapConstructor;

export interface BladeSchemaAttribute {
  type: BladeItemType | BladeCollectionType;
  itemType?: BladeItemType;
  required?: boolean;
  value?: (operation: BladeOperation, value: any) => any;
}

export type BladeSchemaKey<Schema extends BladeSchema> = {
  hashKey: (item: RequiredBladeItem<Schema>) => any;
  sortKey?: (item: RequiredBladeItem<Schema>) => any;
};

export type BladeSchema = Record<string, BladeItemType | BladeSchemaAttribute>;

export type TypeFromItemType<T extends BladeItemType> =
  T extends StringConstructor
    ? string
    : T extends NumberConstructor
    ? number
    : T extends BooleanConstructor
    ? boolean
    : T extends DateConstructor
    ? Date
    : T extends BufferConstructor
    ? Buffer
    : never;

export type TypeFromSchema<T extends BladeSchemaAttribute> =
  T["type"] extends ArrayConstructor
    ? Array<TypeFromItemType<T["itemType"]>>
    : T["type"] extends SetConstructor
    ? Set<TypeFromItemType<T["itemType"]>>
    : T["type"] extends MapConstructor
    ? Map<string, TypeFromItemType<T["itemType"]>>
    : T["type"] extends BladeItemType
    ? TypeFromItemType<T["type"]>
    : never;

export type BladeSchemaRequiredAttribute<T> = T extends BladeSchemaAttribute
  ? T["required"] extends true
    ? true
    : false
  : false;

export type RequiredBladeItem<Schema extends BladeSchema> = {
  [Key in keyof Schema as BladeSchemaRequiredAttribute<Schema[Key]> extends true
    ? Key
    : never]: Schema[Key] extends BladeItemType
    ? TypeFromItemType<Schema[Key]>
    : Schema[Key] extends BladeSchemaAttribute
    ? TypeFromSchema<Schema[Key]>
    : never;
};

export type OptionalBladeItem<Schema extends BladeSchema> = {
  [Key in keyof Schema as BladeSchemaRequiredAttribute<Schema[Key]> extends true
    ? never
    : Key]?: Schema[Key] extends BladeItemType
    ? TypeFromItemType<Schema[Key]>
    : Schema[Key] extends BladeSchemaAttribute
    ? TypeFromSchema<Schema[Key]>
    : never;
};

export type BladeItem<Schema extends BladeSchema> = OptionalBladeItem<Schema> &
  RequiredBladeItem<Schema>;

export type UpdateBladeItem<Schema extends BladeSchema> =
  | Partial<BladeItem<Schema>>
  | { $add: Partial<BladeItem<Schema>> }
  | { $set: Partial<BladeItem<Schema>> }
  | { $remove: Record<keyof Schema, boolean> }
  | { $delete: Partial<BladeItem<Schema>> };

export type ValueCondition =
  | "="
  | "!="
  | ">"
  | ">="
  | "<"
  | "<="
  | "BETWEEN"
  | "BEGINS_WITH"
  | "IN";

export type DataCondtion =
  | "ATTRIBUTE_EXISTS"
  | "ATTRIBUTE_NOT_EXISTS"
  | "ATTRIBUTE_TYPE"
  | "CONTAINS"
  | "SIZE"
  | "SIZE_GT"
  | "SIZE_LT";

export type BladeViewCondition = ValueCondition | DataCondtion;

export type BladeViewField<Schema extends BladeSchema> = string & keyof (Schema & { HASH: any, SORT: any })

export interface BladeResult<Schema> {
  items: Array<Schema>;
  next?: string;
}
