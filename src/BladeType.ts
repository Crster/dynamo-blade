import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
export class PrimaryKeyConstructor {}

export const PrimaryKey = new PrimaryKeyConstructor();

export type BasicType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | BufferConstructor
  | DateConstructor;

export type ComplexType = ArrayConstructor | SetConstructor | MapConstructor;

type FilterType<T, Filter> = T extends Filter ? true : false;

export interface BladeSchema {
  table: {
    name: string;
    hashKey: string;
    sortKey: string;
    typeKey: string;
    createdOn?: string;
    modifiedOn?: string;
  };
  type: Record<string, BladeType<any>>;
  index?: Record<
    string,
    {
      type: "LOCAL" | "GLOBAL";
      hashKey?: string;
      sortKey?: string;
    }
  >;
}

export interface BladeOption<Schema extends BladeSchema> {
  client: DynamoDBDocumentClient;
  schema: Schema;
}

export type TypeFromBasicType<T extends BasicType> = T extends StringConstructor
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

export interface FieldType {
  type: PrimaryKeyConstructor | BasicType | ComplexType;
  itemType?: BasicType;
  required?: boolean;
  default?: any;
}

export type TypeFromFieldType<T extends FieldType> =
  T["type"] extends ArrayConstructor
    ? Array<TypeFromBasicType<T["itemType"]>>
    : T["type"] extends SetConstructor
    ? Set<TypeFromBasicType<T["itemType"]>>
    : T["type"] extends MapConstructor
    ? Map<string, TypeFromBasicType<T["itemType"]>>
    : T["type"] extends BasicType
    ? TypeFromBasicType<T["type"]>
    : never;

export type BladeTypeField<Type> = {
  [Key in keyof Type as FilterType<Type[Key], BladeType<any>> extends true
    ? Key
    : never]: Type[Key];
};

export type BladeTypeAdd<Type> = {
  [Key in keyof Type as FilterType<
    Type[Key],
    BasicType | ComplexType
  > extends true
    ? Key
    : never]?: Type[Key] extends FieldType
    ? TypeFromFieldType<Type[Key]>
    : Type[Key] extends BasicType
    ? TypeFromBasicType<Type[Key]>
    : never;
};

export type BladeTypeUpdate<Type> =
  | BladeTypeAdd<Type>
  | { $add: BladeTypeAdd<Type> }
  | { $set: BladeTypeAdd<Type> }
  | { $remove: Record<keyof Type, boolean> }
  | { $delete: BladeTypeAdd<Type> };

export class BladeType<
  Type extends Record<
    string,
    PrimaryKeyConstructor | BasicType | FieldType | BladeType<any>
  >
> {
  public readonly type: Type;

  constructor(type: Type) {
    this.type = type;
  }
}

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

export type Condition = {
  field: string;
  condition: ValueFilter | DataFilter;
  value?: any;
};
