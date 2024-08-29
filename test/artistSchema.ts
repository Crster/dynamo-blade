import BladeSchema from "../src/BladeSchema";

export const artistSchema = new BladeSchema({
  hashKey: (ii) => `artist#${ii.ArtistId}`,
  sortKey: (ii) => `artist#${ii.ArtistId}`,
  keyAttributes: ["ArtistId"],
  attributes: {
    ArtistId: String,
    Name: String,
    Age: Number,
  },
});
