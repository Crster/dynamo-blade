import { BladeType, PrimaryKey } from "../src";
import { userLike } from "./userLike";

export const user = new BladeType({
  userId: PrimaryKey,
  email: String,
  name: String,
  password: Buffer,
  likes: userLike,
});
