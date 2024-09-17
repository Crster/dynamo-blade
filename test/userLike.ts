import { BladeType, PrimaryKey } from "../src";

export const userLike = new BladeType({
  likeId: PrimaryKey,
  songId: String,
});
