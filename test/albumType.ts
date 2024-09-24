import { BladeAttribute, PrimaryKey } from "../src";
import { song } from "./songType";

export const album = new BladeAttribute({
  albumId: PrimaryKey(String),
  title: String,
  releaseDate: Date,
  song,
});
