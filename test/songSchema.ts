import BladeSchema from "../src/BladeSchema";

export const songSchema = new BladeSchema({
  PK: {
    type: String,
    value: (ii) => `artist#${ii.ArtistId}`,
  },
  SK: {
    type: String,
    value: (ii) => `song#${ii.SongId}`,
  },
  ArtistId: String,
  SongId: String,
}, ["ArtistId", "ArtistId"]);
