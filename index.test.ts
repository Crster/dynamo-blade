import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "./src/index";
import BladeSchema from "./src/BladeSchema";

const songSchema = new BladeSchema({
  hashKey: (ii) => `artist#${ii.ArtistId}:albumId#${ii.AlbumId}`,
  sortKey: (ii) => `songId#${ii.SongId}`,
  keyAttributes: ["ArtistId", "AlbumId", "SongId"],
  attributes: {
    ArtistId: String,
    AlbumId: String,
    SongId: String,
    Title: String,
    Length: Number,
    Model: {
      type: String
    },
  },
});

const db = new DynamoBlade({
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
    song: songSchema,
  },
});

const result = await db
  .open("song")
  .is({ ArtistId: "Hahaha", AlbumId: "Test", SongId: "555" })
  .get();