import { BladeType, PrimaryKey } from "../src";
import { album } from "./albumType";

export const artist = new BladeType({
  artistId: PrimaryKey,
  name: { type: String, required: true },
  age: Number,
  genres: { type: Set, itemType: String },
  album,
});
