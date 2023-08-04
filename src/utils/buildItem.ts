import BladeOption from "../BladeOption";

export default function buildItem<T>(
  value: Record<string, any>,
  option: BladeOption
) {
  if (value) {
    const ret = new Map<string, any>();

    for (const key in value) {
      if (key === option.getFieldName("HASH")) {
        ret.set(key, value[option.getFieldName("SORT_INDEX")]);
      } else if (key === option.getFieldName("SORT")) {
        // Not implemented
      } else if (key === option.getFieldName("HASH_INDEX")) {
        // Not implemented
      } else if (key === option.getFieldName("SORT_INDEX")) {
        // Not implemented
      } else {
        ret.set(key, value[key]);
      }
    }

    return Object.fromEntries(ret) as T;
  } else {
    return null;
  }
}
