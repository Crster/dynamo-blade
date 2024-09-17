import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import DynamoBlade from "../src/";
import { artist } from "./artistType";
import { user } from "./userType";

export const db = new DynamoBlade({
  client: DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  ),
  schema: {
    table: {
      name: "test_db",
      hashKey: "pk",
      sortKey: "sk",
      typeKey: "tk",
      createdOn: "createdOn",
      modifiedOn: "modifiedOn",
    },
    index: {
      byType: {
        type: "GLOBAL",
        hashKey: ["tk", "S"],
        sortKey: ["sk", "S"],
      },
    },
    type: {
      artist,
      user,
    },
  },
});
