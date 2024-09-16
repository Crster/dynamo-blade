import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { BladeType, PrimaryKey } from "./src/BladeType";
import DynamoBlade from "./src/DynamoBlade";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

async function main() {
  const song = new BladeType({
    songId: PrimaryKey,
    title: String,
    length: Number,
  });

  const album = new BladeType({
    albumId: PrimaryKey,
    title: String,
    song,
  });

  const artist = new BladeType({
    artistId: PrimaryKey,
    name: String,
    age: String,
    album,
  });

  const concert = new BladeType({
    concertId: PrimaryKey,
    artistId: String,
    date: Date,
  });

  const db = new DynamoBlade({
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
          hashKey: "tk",
          sortKey: "sk",
        },
      },
      type: {
        artist,
        concert,
      },
    },
  });

  const test = await db
    .open("artist")
    .is("art001")
    .open("album")
    .is("alb001")
    .open("song")
    .is("s001")
    .set({
      $add: {
        length: 1,
      },
    });

  console.log(test);
}

main();
