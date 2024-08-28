import BladeOption from "../BladeOption";
import { BladeItem, Option, QueryResult } from "../BladeType";
import buildItem from "./buildItem";

export default function buildItems<
  Opt extends Option,
  Collection extends keyof Opt["schema"]
>(values: Array<Record<string, any>>, next: string, option: BladeOption<Opt>) {
  const ret: QueryResult<BladeItem<Opt, Collection>> = { items: [] };

  if (values && Array.isArray(values) && values.length > 0) {
    for (const value of values) {
      ret.items.push(buildItem<Opt, Collection>(value, option));
    }

    if (next) {
      ret.next = next;
    }
  }

  return ret;
}
