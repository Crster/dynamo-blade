# Dynamo Blade
## Simple dynamodb client with single table design

#Not yet for production use

### To use
```js
import DynamoBlade from "@crster/dynamo-blade";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { testDb } from "./testDb";

const blade = new DynamoBlade(
  DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  )
);

const db = blade.table(testDb); // Note: must add BladeTable to DynamoBlade to use it
```

### To define table schema
```js
export const testDb = new BladeTable("testDb", { //<---- the name of table
    keySchema: {
      pk: HashKey(String), //<---- Mark pk as the hashkey of the table
      sk: SortKey(String), //<---- Mark sk as the sortkey of the table
      tk: TypeKey(),       //<---- Mark tk as typekey. this will save the document attribute class
      createdOn: CreatedOn(),   //<--- Add createdOn timestamp
      modifiedOn: ModifiedOn(), //<--- Add modifiedOn timestamp
    },
    index: {
      byRelease, //<---- BladeIndex(local) instance
      byType,    //<---- BladeIndex(global) instance
    },
    attribute: {
      artist, //<------- BladeAttribute instance
    },
  })
```

### To define BladeAttribute
```js
export const album = new BladeAttribute({
  albumId: PrimaryKey(String), //<------ The primary key of the attribute
  title: RequiredField(String), //<----------- Mark title as a required field of type string
  releaseDate: Default(() => new Date(2000)), //<--- Use this default value if undefined upon creation only. will not set default value for modify event.
  song, //<--- Sub document of this schema
});

export const artist = new BladeAttribute({
  artistId: PrimaryKey(String), //<------ The primary key of the attribute
  name: String, //<------- Scalar type see below for other supported Scalar type
  age: Number,
  genres: SetField(String), //<---------- Mark genres field type as Set<string>
  album, //<----------------------------- This is a sub document defination. It is an instance of BladeAttribute
});
```

### List if BladeAttribute field types
```js
Scalar Types: String, Number, Boolean, Buffer, Date, OptionalField(), RequiredField()
Key Types: PrimaryKey(), HashKey(), SortKey(), TypeKey(),
Event Types: CreatedOn(), ModifiedOn(), OnCreate(), Default()
Document Types: SetField(), ListField(), DocumentField()

SetField is for defining sets of the same type
ListField is for defining arrays of different item type
DocumentField is for defining map type
```

### To initialize table
```js
// Only use init if you want to create tables. You can use the library without calling this
const [result] = await blade.init();
```

### To add
```js
// The add method will return true when success
const result = await db.open("artist").is("john").add({
    name: "John Doe",
    age: 10,
  });
```

### To get
```js
// The get method will return artist document
const result = await db.open("artist").is("john").get();

console.log(result)
```

### To add sub item
```js
const result = await db.open("artist").is("john").open("album").is("j2").add({
    title: "J2",
    price: 150,
    rating: 3,
  });
```

### To update
```js
const result = await db
    .open("artist")
    .is("john")
    .open("album")
    .is("love1")
    .set({ price: 210 });
```

### To query local index
```js
// query byRelease index where pk = artist#john and releaseDate between 2007-01 and 2007-02
 const result = await db
    .query("byRelease")
    .where({ // Note: the db.query.where method should only contain key field of the index
      pk: db.open("artist").is("john"),
      releaseDate: BladeFilter("BETWEEN", [
        new Date(2007, 1),
        new Date(2007, 2),
      ]),
    })
    .get();
```

### To query global index
```js
const result = await db
    .query("byType")
    .where({
      tk: BladeFilter("=", "song"), // Use BladeFilter to define the condition of the query
    })
    .get();
```