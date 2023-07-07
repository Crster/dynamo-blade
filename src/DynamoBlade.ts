import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import DynamoBladeCollection from "./DynamoBladeCollection";

type Option = {
  tableName: string;
  client: DynamoDBClient;
  hashKey?: string;
  sortKey?: string;
  indexName?: string;
  separator?: string;
};

export default class DynamoBlade {
  public option: Option;

  constructor(option: Option) {
    if (!option) throw new Error("Option is required");
    if (!option.tableName) throw new Error("option.tableName is required");
    if (!option.client) throw new Error("option.client is required");

    this.option = {
      tableName: option.tableName,
      client: option.client,
      hashKey: option.hashKey || "PK",
      sortKey: option.sortKey || "SK",
      indexName: option.indexName || "GS1",
      separator: option.separator || "#",
    };
  }

  open(collection: string) {
    return new DynamoBladeCollection(this, [], collection);
  }
}
