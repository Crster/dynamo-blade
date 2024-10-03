import { BladeAttribute, Override, PrimaryKey } from "../src";
import { userLike } from "./userLike";

export const user = new BladeAttribute({
  pk: Override(() => "user"),
  sk: Override((item) => item.userId),
  allowed: Override(() => true),
  userId: PrimaryKey(String),
  email: String,
  name: String,
  password: Buffer,
  likes: userLike,
});
