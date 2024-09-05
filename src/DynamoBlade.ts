import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import BladeCollection, { BladeCollectionSchema, BladeCollectionSchemaAttributes } from "./BladeCollection";

export type DynamoBladeItemType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | BufferConstructor
  | DateConstructor;

export type DynamoBladeCollectionType = ArrayConstructor | SetConstructor | MapConstructor;

export interface DynamoBladeKey {
  field: string;
  type: DynamoBladeItemType;
}

export interface DynamoBladeIndex {
  hashKey: DynamoBladeKey;
  sortKey?: DynamoBladeKey;
  type: "LOCAL" | "GLOBAL";
  projection?: "ALL" | "KEYS" | Array<string>;
  provision?: { read: number; write: number };
}

export interface DynamoBladeSchema {
  hashKey: DynamoBladeKey;
  sortKey?: DynamoBladeKey;
  index?: Record<string, DynamoBladeIndex>;
}

export interface DynamoBladeOption {
  table: string;
  client: DynamoDBDocumentClient;
  schema: DynamoBladeSchema;
}

export default class DynamoBlade {
  public readonly option: DynamoBladeOption;

  constructor(option: DynamoBladeOption) {
    this.option = option;
  }

  async init() {}

  open<Schema extends BladeCollectionSchemaAttributes>(
    collection: string,
    schema: BladeCollectionSchema<Schema>
  ) {
    return new BladeCollection(this.option, collection, schema);
  }
}
