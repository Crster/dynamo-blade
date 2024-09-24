import { BladeField, BladeFieldKind, OptionalField } from "./BladeField";

export type ScalarType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | BufferConstructor
  | DateConstructor;

export type EventHandler = (value: any) => any;

export type TypeFromScalarType<T extends ScalarType> =
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

export type TypeFromBladeAttributeSchema<T> = T extends ScalarType
  ? TypeFromScalarType<T>
  : T extends BladeField
  ? TypeFromBladeField<T>
  : T extends BladeAttribute<BladeAttributeSchema>
  ? BladeItem<T>
  : never;

export type TypeFromBladeField<T extends BladeField> = T["kind"] extends "List"
  ? Array<TypeFromBladeAttributeSchema<T["type"]>>
  : T["kind"] extends "Set"
  ? Set<TypeFromBladeAttributeSchema<T["type"]>>
  : T["kind"] extends "Document"
  ? Map<string, TypeFromBladeAttributeSchema<T["type"]>>
  : T["kind"] extends "OnCreate"
  ? T["type"] extends EventHandler
    ? ReturnType<T["type"]>
    : never
  : T["type"] extends ScalarType
  ? TypeFromScalarType<T["type"]>
  : never;

export type IsRequiredField<Field> = Field extends BladeField
  ? Field["required"] extends true
    ? true
    : false
  : false;

export type IsKeyField<Field> = Field extends BladeField
  ? Field["kind"] extends "PrimaryKey"
    ? true
    : Field["kind"] extends "HashKey"
    ? true
    : Field["kind"] extends "SortKey"
    ? true
    : Field["kind"] extends "TypeKey"
    ? true
    : Field["kind"] extends "OnModify"
    ? true
    : false
  : false;

export type PrimaryKeyField<
  Schema extends BladeAttribute<BladeAttributeSchema>
> = {
  [Key in keyof Schema["schema"] as Schema["schema"][Key] extends BladeField
    ? Schema["schema"][Key]["kind"] extends "PrimaryKey"
      ? Key
      : never
    : never]: TypeFromBladeAttributeSchema<Schema["schema"][Key]>;
};

export type RequiredField<Schema extends BladeAttribute<BladeAttributeSchema>> =
  {
    [Key in keyof Schema["schema"] as Schema["schema"][Key] extends BladeAttribute<BladeAttributeSchema>
      ? never
      : IsKeyField<Schema["schema"][Key]> extends true
      ? never
      : IsRequiredField<Schema["schema"][Key]> extends true
      ? Key
      : never]: TypeFromBladeAttributeSchema<Schema["schema"][Key]>;
  };

export type OptionalField<Schema extends BladeAttribute<BladeAttributeSchema>> =
  {
    [Key in keyof Schema["schema"] as Schema["schema"][Key] extends BladeAttribute<BladeAttributeSchema>
      ? never
      : IsKeyField<Schema["schema"][Key]> extends true
      ? never
      : IsRequiredField<Schema["schema"][Key]> extends true
      ? never
      : Key]?: TypeFromBladeAttributeSchema<Schema["schema"][Key]>;
  };

export type BladeSchema<
  Attribute extends BladeAttribute<BladeAttributeSchema>
> = {
  [Key in keyof Attribute["schema"] as Attribute["schema"][Key] extends BladeAttribute<BladeAttributeSchema>
    ? Key
    : never]: Attribute["schema"][Key] extends BladeAttribute<BladeAttributeSchema>
    ? Attribute["schema"][Key]
    : never;
};

export type BladeItem<Attribute extends BladeAttribute<BladeAttributeSchema>> =
  OptionalField<Attribute> &
    RequiredField<Attribute> &
    PrimaryKeyField<Attribute>;

export type BladeAttributeSchemaValueType =
  | ScalarType
  | BladeField
  | BladeAttribute<BladeAttributeSchema>;

export type BladeAttributeSchema = Record<
  string,
  BladeAttributeSchemaValueType
>;

export class BladeAttribute<Schema extends BladeAttributeSchema> {
  public readonly schema: Schema;

  constructor(schema: Schema) {
    this.schema = schema;
  }
}
