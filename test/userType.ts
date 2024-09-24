import { BladeAttribute, PrimaryKey } from "../src";
import { userLike } from "./userLike";

export const user = new BladeAttribute({
  userId: PrimaryKey(String),
  email: String,
  name: String,
  password: Buffer,
  likes: userLike,
});
