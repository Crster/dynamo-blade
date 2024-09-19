import { db } from "./test/db";

main()
  .then(() => console.log("Done!"))
  .catch((err) => console.log("Error: " + err.message, err));

//---------------- Testing
async function main() {
  // await db
  //   .open("artist")
  //   .is("cat")
  //   .add({
  //     name: "Cool",
  //     age: 5,
  //     genres: new Set(["pop"]),
  //   }, true);

  // await db
  //   .open("artist")
  //   .is("cat")
  //   .set(
  //     {
  //       name: "Cool",
  //       age: 5,
  //       genres: new Set(["pop"]),
  //     },
  //     ["age", ">", 20]
  //   );

  // await db
  //   .open("artist")
  //   .is("cat")
  //   .open("album")
  //   .is("meow")
  //   .add({
  //     title: "House Cat",
  //     releaseDate: new Date(2004, 5, 2),
  //   });

  // await db
  //   .open("artist")
  //   .is("cat")
  //   .open("album")
  //   .is("roar")
  //   .open("song")
  //   .is("tiger")
  //   .add({
  //     title: "Tiger Cat",
  //     collab: ["Cat", "Dog"],
  //     downloadable: true,
  //     length: 5,
  //   });

  // await db
  //   .open("artist")
  //   .is("cat")
  //   .open("album")
  //   .is("meow")
  //   .set({
  //     releaseDate: new Date(2004, 2, 22),
  //   });

  // await db
  //   .open("artist")
  //   .is("cat")
  //   .open("album")
  //   .is("meow")
  //   .open("song")
  //   .is("tiger")
  //   .add({
  //     title: "Tiger Cat",
  //     collab: ["Cat", "Dog"],
  //     downloadable: true,
  //     length: 4,
  //   });

  // await db
  //   .open("artist")
  //   .is("cat")
  //   .open("album")
  //   .is("meow")
  //   .open("song")
  //   .is("tiger")
  //   .set({
  //     $add: {
  //       length: 6,
  //     },
  //     $remove: {
  //       downloadable: true,
  //     },
  //     $set: {
  //       collab: ["Cat", "Hen", "Dog"],
  //     },
  //     title: "Tiger Kitty",
  //   });

  // await db
  //   .open("artist")
  //   .is("dog")
  //   .open("album")
  //   .is("roff")
  //   .open("song")
  //   .is("chuchu")
  //   .set({
  //     $add: {
  //       length: 2,
  //     },
  //     $remove: {
  //       downloadable: false,
  //     },
  //     $set: {
  //       collab: ["Dog", "Cat"],
  //     },
  //     title: "Cats and Dog",
  //   });

  // await db
  //   .open("artist")
  //   .is("cat")
  //   .open("album")
  //   .is("roar")
  //   .open("song")
  //   .is("tiger")
  //   .remove();

  //const result = await db.open("artist").beginsWith("ca").get();
  //console.log(result);

  // const result2 = await db.open("artist").is("dog").open("album").is("roff").open("song").between("c", "d").get()
  // console.log(result2);

  const result = await db.query("byType").where({
    tk: ["=", "song"],
    
  }).get()
  console.log(result)
}
