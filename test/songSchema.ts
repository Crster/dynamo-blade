import BladeTable from "../src/BladeTable";

export const songSchema = new BladeTable(
  {
    ArtistId: {
      type: String,
      required: true,
      index: true,
    },
    AlbumId: {
      type: String,
      required: true,
      index: true,
    },
    SongId: {
      type: String,
      required: true,
    },
    Title: String,
  },
  {
    HashKey: (ii) => `artist#${ii.ArtistId}`,
    SortKey: (ii) => `song#${ii.SongId}`,
  }
);
