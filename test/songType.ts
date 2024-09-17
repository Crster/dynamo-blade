import { BladeType, PrimaryKey } from "../src";

export const song = new BladeType({
  songId: PrimaryKey,
  title: {
    type: String,
    required: true,
  },
  length: Number,
  downloadable: Boolean,
  collab: { type: Array, itemType: String },
});
