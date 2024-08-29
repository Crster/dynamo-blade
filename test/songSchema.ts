import BladeSchema from "../src/BladeSchema";

export const songSchema = new BladeSchema({
  hashKey: (ii) => `artist#${ii.ArtistId}:albumId#${ii.AlbumId}`,
  sortKey: (ii) => `songId#${ii.SongId}`,
  keyAttributes: ["ArtistId", "AlbumId", "SongId"],
  attributes: {
    ArtistId: String,
    AlbumId: String,
    SongId: String,
    Title: String,
    Length: Number,
  },
});
