import { BladeItem, OptionalBladeItem, RequiredBladeItem } from "./BladeItem";
import BladeOption from "./BladeOption";

export default class BladeDocument<
  Option extends BladeOption,
  SchemaKey extends keyof Option["schema"],
  Key extends RequiredBladeItem<Option["schema"][SchemaKey]>
> {
  private readonly option: Option;
  private readonly schemaKey: SchemaKey;
  private readonly key: Key;

  constructor(option: Option, schemaKey: SchemaKey, key: Key) {
    this.option = option;
    this.schemaKey = schemaKey;
    this.key = key;
  }

  async add(value: OptionalBladeItem<Option["schema"][SchemaKey]>) {
    return {} as BladeItem<Option["schema"][SchemaKey]>;
  }

  async remove() {

  }

  async update(value: OptionalBladeItem<Option["schema"][SchemaKey]>) {
    
  }
}
