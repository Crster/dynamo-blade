import DynamoBlade from "../DynamoBlade";

export default function buildKey(
  blade: DynamoBlade,
  namespace: Array<string>,
) {
  const ret = {
    useIndex: false,
    keys: [],
    collections: [],
    hashKey: {
      name: blade.option.hashKey,
      value: "",
    },
    sortKey: {
      name: blade.option.sortKey,
      value: "",
    },
  };

  const groupKey: Array<{ collection: string; key: string }> = [];
  for (let index = 0; index < namespace.length; index++) {
    const element = namespace[index];

    if (index % 2) {
      ret.keys.push(element);
      groupKey[(index / 2) | 0].key = element;
    } else {
      ret.collections.push(element);
      groupKey.push({ collection: element, key: "" });
    }
  }

  for (let index = 0; index < groupKey.length; index++) {
    const element = groupKey[index];

    if (ret.sortKey.value) {
      ret.sortKey.value += `:${element.collection}${element.key}`;
    } else if (ret.hashKey.value) {
      ret.sortKey.value = `${element.collection}${element.key}`;
    } else {
      ret.hashKey.value = `${element.collection}${element.key}`;
    }
  }

  if (ret.collections.length === 1 && ret.keys.length === 1) {
    ret.sortKey.value = ret.hashKey.value
  } else if (ret.keys.length === 0) {
    ret.hashKey.name = `${blade.option.indexName}${blade.option.hashKey}`
    ret.sortKey.name = `${blade.option.indexName}${blade.option.hashKey}`
    ret.useIndex = true;
  }

  return ret;
}
