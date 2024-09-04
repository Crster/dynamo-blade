import { db } from "./test/dbInstance";

test("add artist", async () => {
  const artist1 = await db.open("artist").is({ ArtistId: "Yes" }).add({
    Name: "Akon",
    BirthDate: new Date(),
  });

  db.open("artist").where("oldArtist", )
});
