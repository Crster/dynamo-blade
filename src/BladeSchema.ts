import { SchemaDefinition, SchemaType } from "./BladeType";

interface BladeSchemaParam<
  Schema extends Record<string, SchemaType | SchemaDefinition>,
  Key extends keyof Schema
> {
  hashKey: (item: Schema) => any;
  sortKey?: (item: Schema) => any;
  keyAttributes: Array<Key>;
  attributes: Schema;
}

export default class BladeSchema<
  Schema extends Record<string, SchemaType | SchemaDefinition>,
  Key extends keyof Schema
> {
  public hashKey: BladeSchemaParam<Schema, Key>["hashKey"];
  public sortKey?: BladeSchemaParam<Schema, Key>["sortKey"];
  public attributes: BladeSchemaParam<Schema, Key>["attributes"];
  public keyAttributes: BladeSchemaParam<Schema, Key>["keyAttributes"];

  constructor(params: BladeSchemaParam<Schema, Key>) {
    this.attributes = params.attributes;
    this.keyAttributes = params.keyAttributes;
    this.hashKey = params.hashKey;
    this.sortKey = params.sortKey;
  }

  getItemValue(prop: string, defaultValue: any) {
    if (this.attributes && this.attributes[prop]) {
      let value = defaultValue;
      if (this.attributes[prop]) {
        if (this.attributes[prop]["type"] instanceof String) {
          value = String(value);
        }
        if (this.attributes[prop]["type"] instanceof Number) {
          value = Number(value);
        }
        if (this.attributes[prop]["type"] instanceof Boolean) {
          value = Boolean(value);
        }
        if (this.attributes[prop]["type"] instanceof Date) {
          value = new Date(value);
        }

        if (this.attributes[prop]["value"]) {
          value = this.attributes[prop]["value"](value);
        }
      }

      return value;
    }
  }

  buildItem<T extends Record<string, any>>(data: any): T | undefined {
    if (data) {
      const ret = {};
      const attributeKeys = Object.keys(this.attributes);

      for (let xx = 0; xx < attributeKeys.length; xx++) {
        const attributeKey = attributeKeys[xx];
        const value = this.getItemValue(attributeKey, data[attributeKey]);
        if (value) {
          ret[attributeKey] = value;
        }
      }

      return ret as T;
    }

    return;
  }
}
