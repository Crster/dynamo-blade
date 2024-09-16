import { BladeDocument } from "./BladeDocument";
import { BladeOption, BladeSchema, BladeType, ValueFilter } from "./BladeType";
import BladeView from "./BladeView";

export default class BladeCollection<
  Schema extends BladeSchema,
  Type extends BladeType<any>
> {
  private readonly option: BladeOption<Schema>;
  private readonly key: Array<string>;

  constructor(option: BladeOption<Schema>, key: Array<string>) {
    this.option = option;
    this.key = key;
  }

  is(key: string) {
    return new BladeDocument<Schema, Type>(this.option, [...this.key, key]);
  }

  where(
    field: string & keyof Type["type"],
    condition: ValueFilter,
    value: any
  ) {
    return new BladeView<Schema, Type>(this.option, this.key, "QUERY").where(
      field,
      condition,
      value
    );
  }
}
