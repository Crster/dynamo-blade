import { unflatten } from "flat";
import DynamoBlade from "../DynamoBlade";

export default function buildResult<T>(
  blade: DynamoBlade,
  results: Array<Record<string, any>>
): T {
  const { indexName, hashKey, sortKey } = blade.option;
  const ret = new Map();

  for (let index = 0; index < results.length; index++) {
    const element = new Map(Object.entries(results[index]));

    const pk = element.get(hashKey);
    const sk = element.get(sortKey);
    const gsk = element.get(`${indexName}${sortKey}`);
    const newPath = pk === sk ? pk : `${pk}.${sk.match(/\w+/gm).join(".")}`;

    element.set(hashKey, gsk);
    element.delete(sortKey);
    element.delete(`${indexName}${hashKey}`);
    element.delete(`${indexName}${sortKey}`);

    ret.set(newPath, Object.fromEntries(element));
  }

  return unflatten(Object.fromEntries(ret), { object: true });
}
