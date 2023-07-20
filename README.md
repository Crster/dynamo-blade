# Dynamo Blade
## Simple dynamodb client with single table design

#Not yet for production use

### To Use
```js
import DynamoBlade from "@crster/dynamo-blade";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const db = new DynamoBlade({
  tableName: "simple",
  client: new DynamoDBClient({
    region: "local",
    endpoint: "http://localhost:8000",
  }),
});
```

### To Initialize table
```js
const result = await db.init()
```

### To Add
```js
const result = await db.open("artist").add("john", {
    name: "John Doe",
    age: 10,
  });
```

### To Get
```js
const result = await db.open("artist").is("john").get();

console.log(result.getItem())
```

### To Add separate property
```js
const result = await db.open("artist").is("john").open("album").add("j2", {
    title: "J2",
    price: 150,
    rating: 3,
  });
```

### To get all albums as array
```js
const result = await db.open("artist.album").get()
console.log(result.getItems())
```

### To Update
```js
const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .is("love1")
    .set({ price: 210 });
```

### To use transaction
> for mutation only
```js
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

await db.transact(commands); // use this to execute the transaction

const result = await artistAlbumnDb.get();
const success = result.getItem().songCount === 4 // the transaction succeed
```