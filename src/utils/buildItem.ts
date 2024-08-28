import BladeOption from "../BladeOption";
import { Option } from "../BladeType";

export default function buildItem<Opt extends Option, Collection extends keyof Opt["schema"]>(
  value: Record<string, any>,
  option: BladeOption<Opt>
) {
  if (value) {
    const ret = new Map<string, any>();

    for (const key in value) {
      if (key === option.getFieldName("HASH")) {
        ret.set(key, option.getKey(value[option.getFieldName("SORT")]));
      } else if (key === option.getFieldName("SORT")) {
        if (value[option.getFieldName("HASH")] === value[option.getFieldName("SORT")]) {
          ret.set(key, value[option.getFieldName("HASH")])
        } else {
          ret.set(key, value[option.getFieldName("HASH")] + ":" + value[option.getFieldName("SORT")])
        }
      } else if (key === option.getFieldName("HASH_INDEX")) {
        // Not implemented
      } else if (key === option.getFieldName("SORT_INDEX")) {
        // Not implemented
      } else {
        ret.set(key, value[key]);
      }
    }

    return Object.fromEntries(ret) as Opt["schema"][Collection];
  } else {
    return null;
  }
}
