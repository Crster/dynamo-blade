import {
  OnDemandThroughput,
  Projection,
  ScalarAttributeType,
} from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

export interface PrimaryKeyConstructor {
  new ();
  readonly prototype: "PrimaryKey";
}
export class PrimaryKeyConstructor implements PrimaryKeyConstructor {}

export var PrimaryKey = new PrimaryKeyConstructor();

export type BasicType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | BufferConstructor
  | DateConstructor;

export type ComplexType = ArrayConstructor | SetConstructor | MapConstructor;

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
      hashKey?: [string, ScalarAttributeType];
      sortKey?: [string, ScalarAttributeType];
      projection?: Projection;
      throughput?: OnDemandThroughput;
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
  [Key in keyof Type as Type[Key] extends BladeType<any> ? Key : never]: Record<
    string,
    any
  >;
};

export type BladeTypePrimary<Type> = {
  [Key in keyof Type as Type[Key] extends PrimaryKeyConstructor
    ? Key
    : never]: string;
};

export type RequiredBladeItem<Type> = {
  [Key in keyof Type as Type[Key] extends FieldType
    ? Type[Key]["required"] extends true
      ? Key
      : never
    : never]: Type[Key] extends FieldType
    ? TypeFromFieldType<Type[Key]>
    : Type[Key] extends BasicType
    ? TypeFromBasicType<Type[Key]>
    : string;
};

export type OptionalBladeItem<Type> = {
  [Key in keyof Type as Type[Key] extends FieldType
    ? Type[Key]["required"] extends true
      ? never
      : Key
    : Type[Key] extends BasicType
    ? Key
    : never]?: Type[Key] extends FieldType
    ? TypeFromFieldType<Type[Key]>
    : Type[Key] extends BasicType
    ? TypeFromBasicType<Type[Key]>
    : string;
};

export type BladeItem<Type> = OptionalBladeItem<Type> &
  RequiredBladeItem<Type> &
  BladeTypePrimary<Type>;

export type BladeTypeAdd<Type> = OptionalBladeItem<Type> &
  RequiredBladeItem<Type>;

export type BladeTypeUpdate<Type> =
  | Partial<BladeTypeAdd<Type>>
  | { $add: Partial<BladeTypeAdd<Type>> }
  | { $set: Partial<BladeTypeAdd<Type>> }
  | { $remove: Record<keyof Type, boolean> }
  | { $delete: Partial<BladeTypeAdd<Type>> };

export interface BladeType<
  Type extends Record<
    string,
    PrimaryKeyConstructor | BasicType | FieldType | BladeType<any>
  >
> {
  new ();
  readonly prototype: "BladeType";
}
export class BladeType<
  Type extends Record<
    string,
    PrimaryKeyConstructor | BasicType | FieldType | BladeType<any>
  >
> implements BladeType<any>
{
  public readonly type: Type;

  constructor(type: Type) {
    this.type = type;
  }
}

export interface BladeResult<Type> {
  items: Array<Type>;
  next?: string;
}

export type ValueFilter =
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
