import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "../src/";
import {
  BladeIndex,
  CreatedOn,
  HashKey,
  ModifiedOn,
  SortKey,
  TypeKey,
  BladeTable,
} from "../src/";
import { artist } from "./artistType";
import { user } from "./userType";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

export const db = new BladeTable("test_db", {
  keySchema: {
    pk: HashKey(String),
    sk: SortKey(String),
    tk: TypeKey(),
    createdOn: CreatedOn(),
    modifiedOn: ModifiedOn(),
  },
  index: {
    byType: new BladeIndex("GLOBAL", {
      keySchema: {
        tk: HashKey(String),
        sk: SortKey(String),
      },
    }),
  },
  attribute: {
    artist,
    user,
  },
});

export const blade = new DynamoBlade(
  DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  )
);
