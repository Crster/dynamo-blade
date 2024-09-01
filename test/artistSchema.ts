import BladeSchema from "../src/BladeSchema";

export const artistSchema = new BladeSchema({
  PK: {
    type: String,
    value: (ii) => `artist#${ii.ArtistId}`,
  },
  SK: {
    type: String,
    value: (ii) => `artist#${ii.ArtistId}`,
  },
  ArtistId: String,
  Name: String,
  BirthDate: Date,
  Age: Number
}, ["ArtistId"]);
