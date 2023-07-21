const { default: DynamoBlade } = require("./dist/index");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");

const db = new DynamoBlade({
  tableName: "testdb",
  client: new DynamoDBClient({
    region: "local",
    endpoint: "http://localhost:8000",
  }),
});

test("test init", async () => {
  const result = await db.init();
  expect(result).toBe(true);
});

test("add artist", async () => {
  const cmd = [
    db.open("artist").add("001", {
      name: "Artist 1",
      age: 30,
    }),
    db.open("artist").add("002", {
      name: "Artist 2",
      age: 23,
    }),
    db.open("artist").add("003", {
      name: "Artist 3",
      age: 25,
    }),
  ];

  const results = await Promise.all(cmd);
  expect(results).toStrictEqual([true, true, true]);
});

test("add album", async () => {
  const cmd = [
    db.open("artist").is("001").open("album").add("ab001", {
      title: "Album 001",
      rating: 5,
    }),
    db.open("artist").is("001").open("album").add("ab002", {
      title: "Album 002",
      rating: 5,
    }),
    db.open("artist").is("001").open("album").add("ab003", {
      title: "Album 003",
      rating: 3,
    }),
    db.open("artist").is("002").open("album").add("ab001", {
      title: "Album 001",
      rating: 4,
    }),
    db.open("artist").is("002").open("album").add("ab002", {
      title: "Album 002",
      rating: 3,
    }),
    db.open("artist").is("002").open("album").add("ab003", {
      title: "Album 003",
      rating: 5,
    }),
    db.open("artist").is("003").open("album").add("ab001", {
      title: "Album 001",
      rating: 3,
    }),
  ];

  const results = await Promise.all(cmd);
  expect(results).toStrictEqual([true, true, true, true, true, true, true]);
});

test("add song", async () => {
  const cmd = [
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab001")
      .open("song")
      .add("s1", {
        title: "Song Number 1",
        genre: "Rock",
        length: 5,
        hasCollab: true,
      }),
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab001")
      .open("song")
      .add("s2", {
        title: "Song Number 2",
        genre: "Rock",
        length: 4.5,
        hasCollab: false,
      }),
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab001")
      .open("song")
      .add("s3", {
        title: "Song Number 3",
        genre: "Alternatives",
        length: 4.5,
        hasCollab: false,
      }),
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab002")
      .open("song")
      .add("s4", {
        title: "Song Number 4",
        genre: "Alternatives",
        length: 3,
        hasCollab: false,
      }),
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab002")
      .open("song")
      .add("s5", {
        title: "Song Number 5",
        genre: "Rock",
        length: 3,
        hasCollab: false,
      }),
    db
      .open("artist")
      .is("003")
      .open("album")
      .is("ab001")
      .open("song")
      .add("s1", {
        title: "Song #1",
        genre: "Pop",
        length: 5,
        hasCollab: true,
        collabs: [
          db.open("artist").is("002").toString(),
          db.open("artist").is("001").toString(),
        ],
      }),
    db
      .open("artist")
      .is("003")
      .open("album")
      .is("ab001")
      .open("song")
      .add("s2", {
        title: "Song #2",
        genre: "Rock",
        length: 5,
        hasCollab: true,
        collabs: [
          db.open("artist").is("002").toString(),
          db.open("artist").is("001").toString(),
        ],
      }),
  ];

  const results = await Promise.all(cmd);
  expect(results).toStrictEqual([true, true, true, true, true, true, true]);
});

test("update value", async () => {
  const cmd = [
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab001")
      .set({
        onsale: true,
        $add: {
          rating: -1,
          rating2: 1,
        },
      }),
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab001")
      .set({
        $set: {
          awards: new Set(["VIP1", "VIP2", "ROCK1"]),
          release: new Date().toISOString(),
        },
        $add: {
          rating: -1,
          rating2: 2
        },
      }),
  ];

  const results = await Promise.all(cmd);
  expect(results).toStrictEqual([true, true]);
});

test("remove", async () => {
  const cmd = [
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab001")
      .open("song")
      .is("s6")
      .remove(),
    db
      .open("artist")
      .is("001")
      .open("album")
      .is("ab001")
      .set({
        $delete: {
          awards: new Set(["VIP2"]),
        },
      }),
    db.open("artist").is("001").open("album").is("ab003").remove(),
  ];

  const results = await Promise.all(cmd);
  expect(results).toStrictEqual([true, true, true]);
});

test("get deleted album", async () => {
  const result = await db
    .open("artist")
    .is("001")
    .open("album")
    .is("ab003")
    .get();

  expect(result.hasItem()).toBe(false);
});

test("verify updated album", async () => {
  const result = await db
    .open("artist")
    .is("001")
    .open("album")
    .where("onsale", "=", true);

  expect(result.getItems().at(0)).toHaveProperty(
    "awards",
    new Set(["ROCK1", "VIP1"])
  );
});

test("get all artist", async () => {
  const results = await db.open("artist").get();

  expect(results.getItems().length).toBe(3);
});

test("get all songs", async () => {
  const results = await db.open("artist.album.song").get();

  expect(results.getItems().length).toBe(7);
});

test("get album by field", async () => {
  const result = await db
    .open("artist")
    .is("001")
    .open("album")
    .is("ab001")
    .get(["", "song"]);
  const result2 = await db
    .open("artist")
    .is("001")
    .open("album")
    .is("ab")
    .get();

  expect([
    result.getItems().length,
    result.getItems("song").length,
    result2.getItems().length,
  ]).toStrictEqual([1, 3, 0]);
});

test("get exact result", async () => {
  const result = await db
    .open("artist")
    .is("001")
    .open("album")
    .is("ab001")
    .open("song")
    .is("s1")
    .get();

  expect(result.getResult().Count).toBe(undefined);
});

test("transaction feature", async () => {
  await db
    .open("artist")
    .is("001")
    .open("album")
    .is("ab001")
    .set({ songCount: 0 });

  const artistAlbumnDb = db.open("artist").is("001").open("album").is("ab001");

  const commands = [
    artistAlbumnDb.open("song").is("s1").when("hasCollab", "=", true),
    artistAlbumnDb.open("song").addLater("s6", {
      title: "Song Number 6",
      genre: "Reggae",
      length: 4.5,
      hasCollab: false,
    }),
    artistAlbumnDb.setLater({
      songCount: 4,
    }),
  ];

  await db.transact(commands);

  const result = await artistAlbumnDb.get();
  expect(result.getItem()).toHaveProperty("songCount", 4);
});
