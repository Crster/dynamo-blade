import BladeCollection from "./BladeCollection";
import BladeOption from "./BladeOption";

export default class DynamoBlade<Option extends BladeOption> {
  public readonly option: Option;

  constructor(option: Option) {
    this.option = option;
  }

  open<SchemaKey extends keyof Option["schema"]>(schema: SchemaKey) {
    return new BladeCollection(this.option, schema);
  }
}
