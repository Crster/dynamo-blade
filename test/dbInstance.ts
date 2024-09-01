import DynamoBlade from "../src/DynamoBlade";
import { artistSchema } from "./artistSchema";
import { songSchema } from "./songSchema";

export const db = new DynamoBlade({
  tableName: "testdb",
  schema: {
    artist: artistSchema,
    song: songSchema,
  },
});