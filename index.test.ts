import { BladeResult } from "./src/BladeType";
import { db } from "./test/db";

test("test init", async () => {
  const result = await db.init();
  expect(result).toBe(true);
});

test("test add artist", async () => {
  const result = await db
    .open("artist")
    .is("akon")
    .add({
      name: "Akon Tiam",
      age: 50,
      genres: new Set(["rnb", "pop"]),
    });

  expect(result).toBe(true);
});

test("test get artist", async () => {
  const result = await db.open("artist").is("akon").get();

  expect(result?.artistId).toBe("akon");
});

test("test update artist", async () => {
  await db
    .open("artist")
    .is("akon")
    .set({
      name: "Badara Akon Thiam",
      $set: {
        age: 51,
      },
      $add: {
        genres: new Set(["hip hop"]),
      },
    });

  const result = await db.open("artist").is("akon").get(true);

  expect(result).toMatchObject({
    name: "Badara Akon Thiam",
    age: 51,
    genres: new Set(["rnb", "pop", "hip hop"]),
  });
});

test("test add album and song", async () => {
  let success = false;
  const album = db.open("artist").is("akon").open("album");

  success = await album.is("trouble").add({
    releaseDate: new Date(2007, 1),
    title: "Trouble",
  });
  expect(success).toBe(true);

  success = await album.is("konvicted").add({ releaseDate: new Date(2006, 2) });
  expect(success).toBe(true);

  success = await album
    .is("konvicted")
    .open("song")
    .is("smackthat")
    .set({
      title: "Smack That",
      collab: ["Eminem"],
      downloadable: false,
      length: 3.33,
    });
  expect(success).toBe(true);

  success = await album
    .is("konvicted")
    .open("song")
    .is("icw")
    .set({
      title: "I Can't Wait",
      collab: ["T-Pain"],
      downloadable: true,
      length: 3.46,
    });

  expect(success).toBe(true);
});

test("test query", async () => {
  let result: BladeResult<any>;

  result = await db
    .open("artist")
    .is("akon")
    .open("album")
    .where("albumId", "BEGINS_WITH", "tr")
    .get();

  expect(result.items.length).toBe(1);

  result = await db
    .open("artist")
    .where("artistId", "=", "akon")
    .where("genres", "CONTAINS", "hip hop")
    .get();

  expect(result.items[0]).toMatchObject({
    artistId: "akon",
  });
});

test("test query index byType", async () => {
  const result = await db.query("byType").where("tk", "=", "song").get();
  expect(result.items.length).toBe(2);
});
