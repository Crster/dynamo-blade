import BladeCollection from "./BladeCollection";
import { BladeOption, BladeSchema, BladeTypeField } from './BladeType';

export default class DynamoBlade<Schema extends BladeSchema> {
  public readonly option: BladeOption<Schema>;

  constructor(option: BladeOption<Schema>) {
    this.option = option;
  }

  open<T extends string & keyof BladeTypeField<Schema["type"]>>(type: T) {
    return new BladeCollection<Schema, Schema["type"][T]>(this.option, [type]);
  }
}
