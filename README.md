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