import { BladeItem } from "./BladeItem";
import BladeOption from "./BladeOption";

export default class BladeCollection<
  Option extends BladeOption,
  SchemaKey extends keyof Option["schema"]
> {
  private option: Option;
  private schemaKey: SchemaKey;

  constructor(option: Option, schemaKey: SchemaKey) {
    this.option = option;
    this.schemaKey = schemaKey;
  }

  async add(value: BladeItem<Option["schema"][SchemaKey]>) {
    return {};
  }
}
