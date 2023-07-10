const { default: DynamoBlade } = require("./dist/index");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");

const db = new DynamoBlade({
  tableName: "simple",
  client: new DynamoDBClient({
    region: "local",
    endpoint: "http://localhost:8000",
  }),
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
  expect(result).toHaveProperty("item.artist.PK", "john");
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

test("get artist#john", async () => {
  const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .is("love1")
    .get();
  expect(result).toHaveProperty("item.album.love1.price", 210);
});
