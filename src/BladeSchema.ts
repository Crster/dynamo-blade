export type ItemType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | DateConstructor
  | BufferConstructor;

export interface SchemaDefinition {
  type: ItemType | ArrayConstructor | SetConstructor | MapConstructor;
  itemType?: ItemType;
  value?: (schema: any) => any;
}

export default class BladeSchema<
  Schema extends Record<string, ItemType | SchemaDefinition>
> {
  public schema: Schema;
  public required?: Array<keyof Schema>;

  constructor(schema: Schema, required?: Array<keyof Schema>) {
    this.schema = schema;
    this.required = required;
  }
}
