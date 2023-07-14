import { QueryCommandOutput } from "@aws-sdk/lib-dynamodb";
import { encodeNext } from "./utils";
import DynamoBlade from "./DynamoBlade";

export default class GetResult {
  private _blade: DynamoBlade;
  private _output: QueryCommandOutput;
  private _collections: Array<string>;
  private _key?: string;

  constructor(
    blade: DynamoBlade,
    output: QueryCommandOutput,
    collections: Array<string>,
    key?: string
  ) {
    this._blade = blade;
    this._output = output;
    this._collections = collections;
    this._key = key;
  }

  hasItem(collection?: string) {
    const { indexName, hashKey } = this._blade.option;
    const _collection = collection
      ? [[...this._collections].splice(-1), collection].join(".")
      : this._collections.join(".");
    let ret: boolean = false;

    for (let xx = 0; xx < this._output.Items.length; xx++) {
      const item = this._output.Items[xx];
      if (item[`${indexName}${hashKey}`] === _collection) {
        ret = true;
        break;
      }
    }

    return ret;
  }

  getNext() {
    return encodeNext(this._output.LastEvaluatedKey);
  }

  getResult(): QueryCommandOutput {
    return this._output;
  }

  getItem<T>(collection?: string, key?: string): T {
    const { indexName, hashKey, sortKey } = this._blade.option;
    let _collection: string,
      _key: string,
      ret: T = null;

    if (collection && !key && !this._key) {
      key = collection;
      collection = null;
    }

    if (collection) {
      _collection = [...this._collections, collection].join(".");
    } else {
      _collection = this._collections.join(".");
    }

    if (key) {
      _key = key;
    } else {
      _key = this._key;
    }

    if (_key) {
      for (let xx = 0; xx < this._output.Items.length; xx++) {
        const item = this._output.Items[xx];

        const propertyName = item[`${indexName}${hashKey}`];
        const propertyKey = item[`${indexName}${sortKey}`];

        if (propertyName === _collection && propertyKey === _key) {
          delete item[`${indexName}${hashKey}`];
          delete item[`${indexName}${sortKey}`];
          delete item[sortKey];

          item[hashKey] = propertyKey;

          ret = item as T;
          break;
        }
      }
    }

    return ret;
  }

  getItems<T>(collection?: string): Array<T> {
    const { indexName, hashKey, sortKey } = this._blade.option;
    const _collection = collection
      ? [[...this._collections].splice(-1), collection].join(".")
      : this._collections.join(".");
    let ret: Array<T> = [];

    for (let xx = 0; xx < this._output.Items.length; xx++) {
      const item = this._output.Items[xx];

      const propertyName = item[`${indexName}${hashKey}`];
      const propertyKey = item[`${indexName}${sortKey}`];

      if (propertyName === _collection) {
        delete item[`${indexName}${hashKey}`];
        delete item[`${indexName}${sortKey}`];
        delete item[sortKey];

        item[hashKey] = propertyKey;

        ret.push(item as T);
      }
    }

    return ret;
  }
}
