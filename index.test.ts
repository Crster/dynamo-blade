import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "./src/index";
import BladeSchema from "./src/BladeSchema";

interface Artist {
  ArtistId: string;
  Model: string;
  Name: string;
}

const artistSchema = BladeSchema<Artist>({
  Model: {
    type: "HASH",
    value: "artist",
  },
  ArtistId: {
    type: "SORT",
    value: (ii) => `artist#${ii.ArtistId}`,
  },
  Name: {
    type: String,
  },
});

interface Album {
  AlbumId: string;
  ArtistId: string;
  Title: string;
}

const albumSchema = BladeSchema<Album>({
  AlbumId: {
    type: "SORT",
    value: (ii) => `album#${ii.AlbumId}`,
  },
  ArtistId: {
    type: "HASH",
    value: (ii) => `artist#${ii.ArtistId}`,
  },
  Title: {
    type: String,
  },
});

interface Song {
  ArtistId: string;
  AlbumId: string;
  SongId: string;
  Title: string;
}

const songSchema = BladeSchema<Song>(
  {
    ArtistId: {
      type: "HASH",
      value: (ii) => `artist#${ii.ArtistId}`,
    },
    AlbumId: {
      type: "HASH",
      value: (ii) => `album#${ii.AlbumId}`,
    },
    SongId: {
      type: "SORT",
      value: (ii) => `song#${ii.SongId}`,
    },
    Title: {
      type: String,
    },
  },
  (ii) => !!ii.Title
);

const db = new DynamoBlade({
  tableName: "testdb",
  client: DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  ),
  schema: {
    artist: artistSchema,
    album: albumSchema,
    song: songSchema,
  },
});


const papa = await db.open("artist").is("papa").get()