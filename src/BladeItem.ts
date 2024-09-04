import BladeTable, { ItemType, SchemaDefinition } from "./BladeTable";

export type TypeFromItemType<T extends ItemType> = T extends StringConstructor
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

export type TypeFromSchemaDefinition<T extends SchemaDefinition> =
  T["type"] extends ArrayConstructor
    ? Array<TypeFromItemType<T["itemType"]>>
    : T["type"] extends SetConstructor
    ? Set<TypeFromItemType<T["itemType"]>>
    : T["type"] extends MapConstructor
    ? Map<string, TypeFromItemType<T["itemType"]>>
    : T["type"] extends ItemType
    ? TypeFromItemType<T["type"]>
    : never;

export type BladeItem<Schema extends BladeTable<any, any>> = {
  [Key in keyof Schema["schema"]]?: Schema["schema"][Key] extends ItemType
    ? TypeFromItemType<Schema["schema"][Key]>
    : TypeFromSchemaDefinition<Schema["schema"][Key]>;
};

export type RequiredBladeItem<Schema extends BladeTable<any, any>> = {
  [Key in keyof Schema["schema"] as Schema["schema"][Key]["required"] extends true
    ? Key
    : never]: Schema["schema"][Key] extends ItemType
    ? TypeFromItemType<Schema["schema"][Key]>
    : TypeFromSchemaDefinition<Schema["schema"][Key]>;
};

export type OptionalBladeItem<Schema extends BladeTable<any, any>> = {
  [Key in keyof Schema["schema"] as Schema["schema"][Key]["required"] extends true
    ? never
    : Key]?: Schema["schema"][Key] extends ItemType
    ? TypeFromItemType<Schema["schema"][Key]>
    : TypeFromSchemaDefinition<Schema["schema"][Key]>;
};

export type BladeItemWithRequired<Schema extends BladeTable<any, any>> =
  BladeItem<Schema> & RequiredBladeItem<Schema>;
