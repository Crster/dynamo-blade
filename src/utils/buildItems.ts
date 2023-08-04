import BladeOption from "../BladeOption";
import { QueryResult } from "../BladeType";
import buildItem from "./buildItem";

export default function buildItems<T>(
  values: Array<Record<string, any>>,
  next: string,
  option: BladeOption
) {
  const ret: QueryResult<T> = { items: [] };

  if (values && Array.isArray(values) && values.length > 0) {
    for (const value of values) {
      ret.items.push(buildItem<T>(value, option));
    }

    if (next) {
      ret.next = next;
    }
  }

  return ret;
}
