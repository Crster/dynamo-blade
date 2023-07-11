const { default: DynamoBlade } = require("./dist/index");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");

const db = new DynamoBlade({
  tableName: "simple_one",
  client: new DynamoDBClient({
    region: "local",
    endpoint: "http://localhost:8000",
  }),
});

test("test init", async () => {
  const result = await db.init()

  expect(result).toBe(true);
});

test("insert artist", async () => {
  const result = await db.open("artist").add("john", {
    name: "John Doe",
    age: 10,
  });

  expect(result).toBe(true);
});

test("get artist#john", async () => {
  const result = await db.open("artist").is("john").get();
  expect(result.getItem()).toHaveProperty("PK", "john");
});

test("insert artist album", async () => {
  const result = await db.open("artist").is("john").open("album").add("j1", {
    title: "J1",
    price: 200,
    rating: 5,
  });

  expect(result).toBe(true);
});

test("insert artist album 2", async () => {
  const result = await db.open("artist").is("john").open("album").add("j2", {
    title: "J2",
    price: 150,
    rating: 3,
  });

  expect(result).toBe(true);
});

test("insert artist album 3", async () => {
  const result = await db.open("artist").is("john").open("album").add("love1", {
    title: "Love J2 Albumn",
    price: 300,
    rating: 4,
  });

  expect(result).toBe(true);
});

test("modify artist album 3", async () => {
  const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .is("love1")
    .set({ price: 210 });

  expect(result).toBe(true);
});

test("get album#love1", async () => {
  const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .is("love1")
    .get();

  expect(result.getItem()).toHaveProperty("price", 210);
});

test("getitem of album", async () => {
  const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .get()

  expect(result.getItem("love1")).toHaveProperty("price", 210);
});

test("getitems of album", async () => {
  const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .get()

  expect(result.getItems().find(ii => ii.PK === "love1")).toHaveProperty("price", 210);
});

test("insert song", async () => {
  const result = await db.open("artist").is("john").open("album").is("love1").open("song").add("wat", {
    title: "What ever!",
    genre: "Rock",
    hasAward: false
  })

  expect(result).toBe(true);
})

test("update song", async () => {
  const result = await db.open("artist").is("john").open("album").is("love1").open("song").is("wat").set({
    title: "What ever! Rock version",
  })

  expect(result).toBe(true);
})

test("getitem song", async () => {
  const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .get()

  expect(result.getItem("song", "wat")).toHaveProperty("genre", "Rock");
});

test("get album love song", async () => {
  const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .get()

  expect(result.getItem("love1")).toHaveProperty("price", 210);
});

test("get all songs", async () => {
  const result = await db.open("artist.album.song").get()

  expect(result.getItems()).toHaveLength(1);
});

test("get all songs", async () => {
  await db.open("artist").is("pre").open("album").add("al1", {
    title: "All love 1",
    price: 123,
    rating: 4,
    hasCollab: true,
  });

  await db.open("artist").is("pre").open("album").add("al2", {
    title: "All love 2",
    price: 234,
    rating: 4,
    hasCollab: true,
  });

  await db.open("artist").is("pre").open("album").add("al3", {
    title: "All love 3",
    price: 345,
    rating: 4,
    hasCollab: true,
  });

  const albums = await db.open("artist.album").where("rating", ">=", 4)
  const promises = []
  for (const album of albums.getItems()) {
    promises.push(db.open("artist").is("pre").open("album").is(album.PK).set({
      hasCollab: false,
      $add: {
        price: 1
      }
    }))
  }

  const results = await Promise.all(promises)

  expect(results).toHaveLength(albums.getResult().Count);
});
