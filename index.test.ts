import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import DynamoBlade from "./src/DynamoBlade";

const db = new DynamoBlade({
  table: "test_db",
  client: DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  ),
  schema: {
    hashKey: { field: "pk", type: String },
    sortKey: { field: "sk", type: String },
    createdOn: true,
    modifiedOn: true,
  },
});

const artist = db.collection(
  {
    id: {
      type: String,
      required: true,
    },
    name: String,
    age: Number,
    model: {
      type: String,
      value: () => "artist",
    },
  },
  {
    hashKey: (ii) => `artist:${ii.id}`,
    sortKey: (ii) => `artist:${ii.id}`,
  }
);

async function main() {
  await db.init();

  const akon = await artist.add({
    id: "akon",
    age: 60,
    name: "Akon",
  });

  const result = await artist.query().where("HASH", "=", { id: "akon" }).get();
  console.log(result);
}

main();
