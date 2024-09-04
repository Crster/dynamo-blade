import { ItemType } from "./BladeTable";

export default class BladeIndex<
  Index extends string,
  Schema extends Record<string, ItemType>,
  SchemaKey extends any
> {
  public readonly index: Index;
  public readonly parameter: Schema;
  public readonly condition: SchemaKey;

  constructor(index: Index, parameter: Schema, condition: SchemaKey) {
    this.index = index;
    this.parameter = parameter;
    this.condition = condition;
  }
}
