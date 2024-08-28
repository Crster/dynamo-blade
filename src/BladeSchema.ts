type Formatter<Schema, Key extends keyof Schema> = (
  item: Schema
) => Schema[Key];

export default function BladeSchema<Schema>(
  schema: {
    [Key in keyof Required<Schema>]: {
      type:
        | "HASH"
        | "SORT"
        | StringConstructor
        | NumberConstructor
        | BufferConstructor
        | BooleanConstructor
        | DateConstructor;
      optional?: boolean;
      value?: string | Formatter<Schema, Key>;
    };
  },
  validator?: (item: Schema) => boolean
) {
  return { schema, validator };
}
