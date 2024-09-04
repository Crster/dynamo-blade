import BladeTable from "../src/BladeTable";
import BladeView from "../src/BladeView";

export const artistSchema = new BladeTable(
  {
    ArtistId: {
      type: String,
      required: true,
    },
    Name: String,
    BirthDate: Date,
    Age: Number,
  },
  {
    HashKey: (ii) => `artist#${ii.ArtistId}`,
    SortKey: (ii) => `artist#${ii.ArtistId}`,
  }
);

export const activeArtist = new BladeView(
  {
    DateFrom: Date,
    DateTo: Date,
  },
  {
    HashKey: ["Age", "BETWEEN", "DateFrom", "DateTo"],
  }
);
