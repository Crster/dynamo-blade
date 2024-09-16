import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { BladeType, PrimaryKey } from "./src/BladeType";
import DynamoBlade from "./src/DynamoBlade";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

async function main() {
  const song = new BladeType({
    songId: PrimaryKey,
    title: {
      type: String,
      required: true,
    },
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
          hashKey: ["tk", "S"],
          sortKey: ["sk", "S"],
        },
      },
      type: {
        artist,
        concert,
      },
    },
  });

  const test = await db.query("byType").where("tk", "=", "artist").get();

  console.log(test.items[0]);
}

main();
