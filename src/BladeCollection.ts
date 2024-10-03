import { Blade } from "./Blade";
import { BladeDocument } from "./BladeDocument";
import { BladeResult, BladeView } from "./BladeView";
import {
  BladeAttribute,
  BladeAttributeSchema,
  BladeItem,
} from "./BladeAttribute";

export class BladeCollection<
  Type extends BladeAttribute<BladeAttributeSchema>
> {
  private readonly blade: Blade<any>;

  constructor(blade: Blade<any>) {
    this.blade = blade;
  }

  is(key: string | number | Date) {
    return new BladeDocument<Type>(this.blade.whereKey("=", key));
  }

  beginsWith(key: string) {
    return new BladeView<Type, BladeResult<Array<BladeItem<Type>>>>(
      this.blade.whereKey("BEGINS_WITH", key),
      {
        count: 0,
        data: [],
      }
    );
  }

  between(fromKey: string, toKey: string) {
    return new BladeView<Type, BladeResult<Array<BladeItem<Type>>>>(
      this.blade.whereKey("BETWEEN", [fromKey, toKey]),
      {
        count: 0,
        data: [],
      }
    );
  }
}
