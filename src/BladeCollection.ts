import { BladeView } from "./BladeView";
import { BladeDocument } from "./BladeDocument";
import { BladeType } from "./BladeType";
import { BladeKeySchema } from "./BladeKeySchema";

export class BladeCollection<Type extends BladeType<any>> {
  private readonly blade: BladeKeySchema<any>;

  constructor(blade: BladeKeySchema<any>) {
    this.blade = blade;
  }

  is(key: string | number | Date) {
    return new BladeDocument<Type>(this.blade.whereKey("=", key));
  }

  beginsWith(key: string) {
    return new BladeView<Type>(this.blade.whereKey("BEGINS_WITH", key));
  }

  between(fromKey: string, toKey: string) {
    return new BladeView<Type>(
      this.blade.whereKey("BETWEEN", [fromKey, toKey])
    );
  }
}
