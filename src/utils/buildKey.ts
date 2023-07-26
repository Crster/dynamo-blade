import DynamoBlade from "../DynamoBlade";

export default function buildKey(blade: DynamoBlade, namespace: Array<string>) {
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

  if (
    groupKey.length == 1 &&
    groupKey[0].collection &&
    groupKey[0].key === ""
  ) {
    ret.useIndex = true;
    ret.hashKey.name = `${blade.option.indexName}${blade.option.hashKey}`;
    ret.sortKey.name = `${blade.option.indexName}${blade.option.sortKey}`;

    ret.hashKey.value = ret.collections.join(".");
    ret.sortKey.value = groupKey
      .map((ii) => `${ii.collection}${ii.key}`)
      .join(":");
  } else {
    if (groupKey.length > 1) {
      ret.sortKey.value = [groupKey.pop()]
        .map((ii) => `${ii.collection}${ii.key}`)
        .join();
    } else {
      ret.sortKey.value = groupKey
        .map((ii) => `${ii.collection}${ii.key}`)
        .join(":");
    }

    ret.hashKey.value = groupKey
      .map((ii) => `${ii.collection}${ii.key}`)
      .join(":");
  }

  return ret;
}
