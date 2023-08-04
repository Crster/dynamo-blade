import BladeOption from "../BladeOption";

export default function buildItem<T>(
  value: Record<string, any>,
  option: BladeOption
) {
  if (value) {
    const ret = new Map<string, any>();

    for (const key in value) {
      if (key === option.getFieldName("HASH")) {
        ret.set(
          option.getFieldName("SORT"),
          option.getFieldValue("PRIMARY_KEY")
        );
      } else if (key === option.getFieldName("SORT")) {
        ret.set(
          option.getFieldName("HASH"),
          value.split(option.separator).pop()
        );
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
