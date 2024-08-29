import { dbInstance as db } from "./test/dbInstance";

test("add artist", async () => {
  const akon = await db.open("artist").add({
    ArtistId: "akon",
    Name: "Akon Thiam",
    Age: 40
  });

  expect(akon?.ArtistId)
});

test("add song", async () => {
  const song1 = await db.open("song").get
})
