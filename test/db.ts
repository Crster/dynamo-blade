import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import DynamoBlade, {
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
import { song } from "./songType";

export const blade = new DynamoBlade(
  DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  )
);

export const db = blade.table(
  new BladeTable("testDb", {
    keySchema: {
      pk: HashKey(String),
      sk: SortKey(String),
      tk: TypeKey(),
      createdOn: CreatedOn(),
      modifiedOn: ModifiedOn(),
    },
    index: {
      byRelease: new BladeIndex("LOCAL", {
        keySchema: {
          pk: HashKey(String),
          releaseDate: SortKey(String),
        },
      }),
      byType: new BladeIndex("GLOBAL", {
        keySchema: {
          tk: HashKey(String),
          sk: SortKey(String),
        },
        attribute: {
          "artist:album:song": song,
        },
      }),
    },
    attribute: {
      artist,
      user,
    },
  })
);
