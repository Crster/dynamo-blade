import { BladeView } from "./BladeView";
import { BladeDocument } from "./BladeDocument";
import { BladeAttribute, BladeAttributeSchema } from "./BladeAttribute";
import { Blade } from "./Blade";

export class BladeCollection<Type extends BladeAttribute<BladeAttributeSchema>> {
  private readonly blade: Blade<any>;

  constructor(blade: Blade<any>) {
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
