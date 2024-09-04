import BladeIndex from "./BladeIndex";

export type ItemType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | DateConstructor
  | BufferConstructor;

export interface SchemaDefinition {
  type: ItemType | ArrayConstructor | SetConstructor | MapConstructor;
  itemType?: ItemType;
  required?: boolean;
  value?: (schema: any) => any;
}

export interface KeySchema<Schema> {
  HashKey: (data: Schema) => any;
  SortKey?: (data: Schema) => any;
}

export default class BladeTable<
  Schema extends Record<string, ItemType | SchemaDefinition>,
  SchemaKey extends KeySchema<Schema>,
  Index extends Record<string, BladeIndex<string, any, any>>
> {
  public readonly schema: Schema;
  public readonly schemaKey: SchemaKey;
  public readonly index?: Index;

  constructor(schema: Schema, schemaKey: SchemaKey, index?: Index) {
    this.schema = schema;
    this.schemaKey = schemaKey;
    this.index = index;
  }
}
