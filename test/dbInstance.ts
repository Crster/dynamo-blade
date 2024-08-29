import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import DynamoBlade from '../src/DynamoBlade';
import { songSchema } from "./songSchema";
import { artistSchema } from "./artistSchema";

export const dbInstance = new DynamoBlade({
  tableName: "testdb",
  client: DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  ),
  primaryKey: {
    hashKey: ["PK", String],
    sortKey: ["SK", String],
    separator: ":",
  },
  schema: {
    artist: artistSchema,
    song: songSchema,
  },
});