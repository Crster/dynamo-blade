import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "./src/DynamoBlade";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const db = new DynamoBlade({
  client: DynamoDBDocumentClient.from(new DynamoDBClient()),
  schema: {
    hashKey: {
      field: "PK",
      type: String,
    },
    sortKey: {
      field: "SK",
      type: String,
    },
    index: {
      model: {
        hashKey: {
          field: "PK",
          type: String
        },
        sortKey: {
          field: "name",
          type: String
        },
        type: "LOCAL"
      },
    },
  },
  table: "db",
});

const artist = db.open("artist", {
  attribute: {
    id: String,
    name: String,
    age: Number,
  },
  key: {
    hashKey: () => `artist`,
    sortKey: (ii) => ii.id,
  },
});

const test = artist.get("");
