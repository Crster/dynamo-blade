import { BladeType, PrimaryKey } from "../src";

export const song = new BladeType({
  songId: PrimaryKey,
  artistAlbum: (ii) => `${ii.artistId}#${ii.albumId}`,
  title: {
    type: String,
    required: true,
  },
  length: Number,
  downloadable: Boolean,
  collab: { type: Array, itemType: String },
});
