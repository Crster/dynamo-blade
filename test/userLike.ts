import { BladeAttribute, PrimaryKey } from "../src";

export const userLike = new BladeAttribute({
  likeId: PrimaryKey(String),
  songId: String,
});
