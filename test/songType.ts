import {
  BladeAttribute,
  Default,
  PrimaryKey,
  RequiredField,
  SetField,
} from "../src";

export const song = new BladeAttribute({
  songId: PrimaryKey(String),
  artistAlbum: Default((ii) => `${ii.artistId}#${ii.albumId}`),
  createdOn: Date,
  title: RequiredField(String),
  length: Number,
  downloadable: Boolean,
  collab: SetField(String),
});