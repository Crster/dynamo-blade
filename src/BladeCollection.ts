import BladeDocument from "./BladeDocument";
import { RequiredBladeItem } from "./BladeItem";
import BladeOption from "./BladeOption";
export default class BladeCollection<
  Option extends BladeOption,
  SchemaKey extends keyof Option["schema"]
> {
  private readonly option: Option;
  private readonly schemaKey: SchemaKey;

  constructor(option: Option, schemaKey: SchemaKey) {
    this.option = option;
    this.schemaKey = schemaKey;
  }

  is(key: RequiredBladeItem<Option["schema"][SchemaKey]>) {
    return new BladeDocument(this.option, this.schemaKey, key);
  }

  where(index: keyof Option["schema"][SchemaKey]["index"]) {
    
  }
}
