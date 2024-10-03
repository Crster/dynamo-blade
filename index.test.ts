import { BladeFilter } from "./src";
import { blade, db } from "./test/db";

test("test init", async () => {
  const [result] = await blade.init();
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

  expect(result).toMatchObject({
    artistId: "akon",
    name: "Akon Tiam",
    age: 50,
    genres: new Set(["rnb", "pop"]),
  });
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

test("test add song", async () => {
  await db
    .open("artist")
    .is("akon")
    .open("album")
    .is("trouble")
    .open("song")
    .is("mamaafrika")
    .add(
      {
        title: "Mama Africa",
        collab: new Set(["akon"]),
        downloadable: true,
        length: 4.26,
      },
      true
    );

  const newSong = await db
    .open("artist")
    .is("akon")
    .open("album")
    .is("trouble")
    .open("song")
    .is("mamaafrika")
    .get();

  expect(newSong).toMatchObject({
    songId: "mamaafrika",
    artistAlbum: "akon#trouble",
  });
});

test("test add album and song", async () => {
  let success = false;
  const album = db.open("artist").is("akon").open("album");

  success = await album.is("trouble").add(
    {
      releaseDate: new Date(2007, 1),
      title: "Trouble",
    },
    true
  );
  expect(success).toBe(true);

  success = await album
    .is("konvicted")
    .add({ releaseDate: new Date(2006, 2) }, true);
  expect(success).toBe(true);

  success = await album
    .is("konvicted")
    .open("song")
    .is("smackthat")
    .set({
      title: "Smack That",
      collab: new Set(["Eminem"]),
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
      collab: new Set(["T-Pain"]),
      downloadable: true,
      length: 3.46,
    });

  expect(success).toBe(true);
});

test("test query", async () => {
  const result = await db
    .open("artist")
    .is("akon")
    .open("album")
    .beginsWith("tr")
    .get();

  expect(result.count).toBe(1);

  const result2 = await db
    .open("artist")
    .is("akon")
    .where("genres", "CONTAINS", "hip hop")
    .get();

  expect(result2.data[0]).toMatchObject({
    artistId: "akon",
  });
});

test("test query local index byReleaseDate", async () => {
  const result = await db
    .query("byRelease")
    .where({
      pk: db.open("artist").is("akon"),
      releaseDate: BladeFilter("BETWEEN", [
        new Date(2007, 1),
        new Date(2007, 2),
      ]),
    })
    .get();

  expect(result.count).toBe(1);
});

test("test query global index byType", async () => {
  const result = await db
    .query("byType")
    .where({
      tk: BladeFilter("=", "artist:album:song"),
    })
    .get();

  expect(result.count).toBe(3);
});

test("test conditional update", async () => {
  const result = await db.open("artist").is("akon").set(
    {
      age: 40,
    },
    db.open("artist").is("akon").where("age", ">", 50)
  );

  expect(result).toBe(true);
});

test("test transaction", async () => {
  const result = await db.transact([
    db.open("artist").is("akon").where("age", "=", 40).condition(),
    db
      .open("artist")
      .is("akon")
      .open("album")
      .is("konvicted")
      .setLater({ title: "Konvicted" }),
    db
      .open("artist")
      .is("iyaz")
      .addLater({
        name: "Keidran Jones",
        age: 37,
        genres: new Set(["pop"]),
      }),
  ]);

  expect(result).toBe(true);
});

test("test override key", async () => {
  const result = await db.open("user").is("admin").add({
    name: "Super Admin",
  });

  const result2 = await db.open("user").is("admin").get(true);

  expect({ success: result, data: result2 }).toMatchObject({
    success: true,
    data: { pk: "user", sk: "admin" },
  });
});

test("test find query", async () => {
  const result = await db
    .query("byType")
    .where({
      tk: BladeFilter("=", "artist:album:song"),
    })
    .find(5);

  expect(result.count).toBe(3);
});
