import BladeSchema, { ItemType, SchemaDefinition } from "./BladeSchema";

export type UnionOfArrayElements<T extends Readonly<any[]>> = T[number];

export type BladeItemType<T extends ItemType> = T extends StringConstructor
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

export type BladeItem<Schema extends BladeSchema<any>> = {
  [Key in keyof Schema["schema"]]: Schema["schema"][Key] extends ItemType
    ? BladeItemType<Schema["schema"][Key]>
    : Schema["schema"][Key] extends SchemaDefinition
    ? Schema["schema"][Key]["type"] extends ArrayConstructor
      ? BladeItemType<Schema["schema"][Key]["itemType"]>
      : Schema["schema"][Key]["type"] extends SetConstructor
      ? BladeItemType<Schema["schema"][Key]["itemType"]>
      : Schema["schema"][Key]["type"] extends MapConstructor
      ? BladeItemType<Schema["schema"][Key]["itemType"]>
      : Schema["schema"][Key]["type"] extends ItemType
      ? BladeItemType<Schema["schema"][Key]["type"]>
      : never
    : never;
};

export type RequiredBladeItem<
  T extends Record<string, ItemType | SchemaDefinition>,
  Schema extends BladeSchema<T>
> = Pick<T, UnionOfArrayElements<Schema["required"]>>;
