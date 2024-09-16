import { BladeDocument } from "./BladeDocument";
import { BladeOption, BladeSchema, BladeType, BladeTypeField } from './BladeType';

export default class BladeCollection<Schema extends BladeSchema, Type extends BladeType<any>> {
  private readonly option: BladeOption<Schema>;
  private readonly key: Array<string>;

  constructor(
    option: BladeOption<Schema>,
    key: Array<string>
  ) {
    this.option = option;
    this.key = key;
  }

  is(key: string) {
    return new BladeDocument<Schema, Type>(
      this.option,
      [...this.key, key]
    );
  }
}
