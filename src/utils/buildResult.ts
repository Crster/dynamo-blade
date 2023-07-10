import { unflatten } from "flat";
import DynamoBlade from "../DynamoBlade";

export default function buildResult<T>(
  blade: DynamoBlade,
  results: Array<Record<string, any>>
): Record<string, Record<string, T>> {
  const { indexName, hashKey, sortKey, separator } = blade.option;
  const ret = new Map();

  for (const result of results) {
    const item = new Map(Object.entries(result));

    const [rootPropertyName, rootPropertyKey] = item
      .get(hashKey)
      .split(separator);
    const propertyName = item.get(`${indexName}${hashKey}`);
    const propertyKey = item.get(`${indexName}${sortKey}`);

    item.delete(`${indexName}${hashKey}`);
    item.delete(`${indexName}${sortKey}`);
    item.delete(sortKey);

    item.set(hashKey, propertyKey);

    if (propertyKey === rootPropertyKey) {
      ret.set(rootPropertyName, Object.fromEntries(item));
    } else {
      ret.set(
        `${propertyName}.${propertyKey}`.replace(rootPropertyName + ".", ""),
        Object.fromEntries(item)
      );
    }
  }

  return unflatten(Object.fromEntries(ret), { object: true });
}
