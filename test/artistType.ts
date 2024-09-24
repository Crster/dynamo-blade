import { BladeAttribute, Default, PrimaryKey, SetField } from "../src";
import { album } from "./albumType";

export const artist = new BladeAttribute({
  artistId: PrimaryKey(String),
  name: String,
  age: Number,
  genres: SetField(String),
  album,
});
