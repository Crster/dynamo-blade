import { BladeType, PrimaryKey } from "../src";
import { song } from "./songType";

export const album = new BladeType({
  albumId: PrimaryKey,
  title: String,
  releaseDate: Date,
  song,
});
