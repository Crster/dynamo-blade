import { RequiredBladeItem, BladeItem } from './BladeItem';
import BladeOption from "./BladeOption";

export default class BladeCollectionView<
  Option extends BladeOption,
  SchemaKey extends keyof Option["view"]
> {
  private readonly option: Option;
  private readonly schemaKey: SchemaKey;

  constructor(option: Option, schemaKey: SchemaKey) {
    this.option = option;
    this.schemaKey = schemaKey;
  }

  is(key: Record<keyof Option["view"][SchemaKey]["parameter"], string>) {
    
  }
}
