import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { FieldType, Option } from "./BladeType";

export default class BladeOption {
  private hashKey: string;
  private sortKey: string;
  private indexName: string;
  private namespace: Map<string, string>;

  public key: string;
  public collection: string;
  public tableName: string;
  public separator: string;
  public client: DynamoDBClient;

  constructor(option: Option) {
    this.client = option.client;
    this.tableName = option.tableName;
    this.hashKey = option.hashKey;
    this.sortKey = option.sortKey;
    this.indexName = option.indexName;
    this.separator = option.separator;
    this.namespace = new Map();
  }

  getFieldName(type: FieldType) {
    switch (type) {
      case "HASH":
        return this.hashKey;
      case "HASH_INDEX":
        return `${this.indexName}${this.hashKey}`;
      case "SORT":
        return this.sortKey;
      case "SORT_INDEX":
        return `${this.indexName}${this.sortKey}`;
      case "INDEX":
        return this.indexName;
      case "PRIMARY_KEY":
        return this.hashKey;
      default:
        return ""
    }
  }

  getFieldValue(type: FieldType) {
    let index = 0;
    const ret: Array<string> = [];

    switch (type) {
      case "HASH":
        for (const [key, value] of this.namespace.entries()) {
          if (index < this.namespace.size) {
            ret.push(`${key}${this.separator}${value}`);
          }

          index++;
        }
        break;
      case "HASH_INDEX":
        for (const key of this.namespace.keys()) {
          ret.push(key);
        }
        break;
      case "SORT":
        for (const [key, value] of this.namespace.entries()) {
          if (index >= this.namespace.size) {
            ret.push(`${key}${this.separator}${value}`);
          }

          index++;
        }
        break;
      case "SORT_INDEX":
        for (const [key, value] of this.namespace.entries()) {
          if (index == 0) {
            ret.push(`${key}${this.separator}${value}`);
          } else if (index >= this.namespace.size) {
            ret.push(`${key}${this.separator}${value}`);
          }

          index++;
        }
        break;
      case "PRIMARY_KEY":
        for (const [key, value] of this.namespace.entries()) {
          ret.push(`${key}${this.separator}${value}`);
        }
        break;
    }

    return ret.join(":");
  }

  openCollection(name: any) {
    const ret = new BladeOption({
      client: this.client,
      tableName: this.tableName,
      hashKey: this.hashKey,
      sortKey: this.sortKey,
      indexName: this.indexName,
      separator: this.separator,
    });

    ret.collection = name;
    ret.namespace.set(name, null);

    return ret;
  }

  openKey(key: any) {
    const ret = new BladeOption({
      client: this.client,
      tableName: this.tableName,
      hashKey: this.hashKey,
      sortKey: this.sortKey,
      indexName: this.indexName,
      separator: this.separator,
    });

    ret.key = key;
    ret.collection = this.collection;
    ret.namespace.set(this.collection, key);

    return ret;
  }
}
