import { FieldType, Option } from "./BladeType";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

export default class BladeOption {
  private hashKey: string;
  private sortKey: string;
  private indexName: string;
  private namespace: Map<string, string>;

  public key: string;
  public collection: string;
  public tableName: string;
  public separator: string;
  public forwardScan: boolean;
  public client: DynamoDBDocumentClient;

  constructor(option: Option) {
    this.client = option.client;
    this.tableName = option.tableName;
    this.hashKey = "PK";
    this.sortKey = "SK";
    this.indexName = "GS1";
    this.separator = "#";
    this.namespace = new Map();
    this.forwardScan = true;
  }

  isUseIndex = () => {
    return this.namespace.size === 1 && !this.key;
  };

  getKey = (primaryKey: string, collection?: string) => {
    let ret: string = null;

    if (primaryKey && primaryKey.includes(this.separator)) {
      const keyvals = primaryKey.split(":");
      for (const keyval of keyvals) {
        const [key, val] = keyval.split(this.separator);
        if (key === collection) {
          ret = val;
          break;
        } else if (!collection) {
          ret = val;
        }
      }
    }

    return ret;
  };

  getFieldName = (type: FieldType) => {
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
        return "";
    }
  };

  getFieldValue = (type: FieldType) => {
    let counter = 1;
    const ret: Array<string> = [];

    switch (type) {
      case "HASH": // Get Right
        if (this.namespace.size <= 1) {
          counter--;
        }

        for (const [key, value] of this.namespace.entries()) {
          if (counter < this.namespace.size) {
            if (value) {
              ret.push(`${key}${this.separator}${value}`);
            } else {
              ret.push(key);
            }
          }

          counter++;
        }
        break;
      case "HASH_INDEX":
        for (const key of this.namespace.keys()) {
          ret.push(key);
        }
        break;
      case "SORT": // Get Last
        if (this.namespace.size <= 1) {
          counter++;
        }

        for (const [key, value] of this.namespace.entries()) {
          if (counter >= this.namespace.size) {
            if (value) {
              ret.push(`${key}${this.separator}${value}`);
            } else {
              ret.push(`${key}${this.separator}`);
            }
          }

          counter++;
        }
        break;
      case "SORT_INDEX": // Get all but not the last - 1
        for (const [key, value] of this.namespace.entries()) {
          if (this.namespace.size == 1) {
            ret.push(value ?? key)
          } else if (counter < this.namespace.size - 1) {
            ret.push(value ?? key);
          } else if (counter >= this.namespace.size) {
            ret.push(value ?? key)
          }

          counter++
        }
        break;
      case "PRIMARY_KEY":
        for (const [key, value] of this.namespace.entries()) {
          if (value) {
            ret.push(`${key}${this.separator}${value}`);
          } else {
            ret.push(key);
          }
        }
        break;
    }

    return ret.join(":");
  };

  openCollection = (name: any) => {
    const ret = new BladeOption({
      client: this.client,
      tableName: this.tableName,
    });

    for (const [key, val] of this.namespace.entries()) {
      ret.namespace.set(key, val);
    }

    ret.collection = name;
    ret.namespace.set(name, null);
    ret.forwardScan = this.forwardScan;

    return ret;
  };

  openKey = (key: any) => {
    const ret = new BladeOption({
      client: this.client,
      tableName: this.tableName,
    });

    for (const [key, val] of this.namespace.entries()) {
      ret.namespace.set(key, val);
    }

    ret.key = key;
    ret.collection = this.collection;
    ret.namespace.set(this.collection, key);
    ret.forwardScan = this.forwardScan;

    return ret;
  };
}
