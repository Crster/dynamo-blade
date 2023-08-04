import BladeOption from "../BladeOption";

export default function buildItem<T>(
  value: Record<string, any>,
  option: BladeOption
) {
  if (value) {
    const ret = new Map<string, any>();

    for (const key in value) {
      if (key === option.getFieldName("HASH")) {
        ret.set(key, option.getKey(value[option.getFieldName("SORT")]));
      } else if (key === option.getFieldName("SORT")) {
        const hashValue = value[option.getFieldName("HASH")];
        const sortValue = value[option.getFieldName("SORT")];

        if (hashValue == sortValue) {
          ret.set(key, hashValue);
        } else {
          ret.set(key, `${hashValue}:${sortValue}`);
        }
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
