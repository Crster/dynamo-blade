# Dynamo Blade
## Simple dynamodb client with single table design

#Not yet for production use

### To Use
```
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

### To Add
```
const result = await db.open("artist").add("john", {
    name: "John Doe",
    age: 10,
  });
```

### To Get
```
const result = await db.open("artist").is("john").get();
```

### To Add separate property
```
const result = await db.open("artist").is("john").open("album").add("j2", {
    title: "J2",
    price: 150,
    rating: 3,
  });
```

### To Update
```
const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .is("love1")
    .set({ price: 210 });
```