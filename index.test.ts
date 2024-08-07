import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import DynamoBlade from "./src/index";

interface Schema {
  sample: string;

  readonly artist: {
    name: string;
    alias?: string;
    age?: number;
    songCount?: number;
    collabs?: Set<string>;

    readonly album: {
      year: number;
      songCount: number;

      readonly song: {
        name?: string;
        title?: string;
        fever?: number;
        totalFever?: number;
        genre?: "pop";
        hasCollab?: boolean;
        collabs?: Array<string>;
        volumn?: number;
        year?: number;
        songCount?: number;
      };
    };

    readonly concert: {
      price: number;
      attendance: number;
    };
  };

  readonly "artist:album": Schema["artist"]["album"];
  readonly "artist:concert": Schema["artist"]["concert"];
}

const db = new DynamoBlade<Schema>({
  tableName: "testdb",
  client: DynamoDBDocumentClient.from(
    new DynamoDBClient({
      region: "us-east-1",
      endpoint: "http://localhost:8000",
    })
  ),
});

test("test init", async () => {
  const result = await db.init();
  expect(result).toBe(true);
});

test("test add artist", async () => {
  const result1 = await db.open("artist").add("akon", {
    name: "Akon Thiam",
    alias: "Akon",
    age: 40,
  });

  expect(result1?.PK).toBe("akon");

  const result2 = await db.open("artist").add("iyaz", {
    name: "Keidran Jones",
    age: 30,
    songCount: 100,
    collabs: new Set(["akon", "madonna", "charice"]),
  });

  expect(result2?.PK).toBe("iyaz");

  const result3 = await db.open("artist").add("charice", {
    name: db.open("artist").is("charice").toString(),
  });

  expect(result3?.PK).toBe("charice");
});

test("get artist by key", async () => {
  const result1 = await db.open("artist").is("artist#akon").get();
  expect(result1).toBe(null);

  const result2 = await db.open("artist").is("akon").get();
  expect(result2).toHaveProperty("PK", "akon");
  expect(result2).toHaveProperty("SK", "artist#akon");
});

test("get artist data", async () => {
  const result = await db.open("artist").is("iyaz").getWith(["album", "concert"])
  expect(result?.items.length).toBe(7)
})

test("update artist age", async () => {
  const result1 = await db
    .open("artist")
    .is("akon")
    .set({
      age: 50,
      $set: {
        songCount: 30,
      },
      $remove: {
        alias: false,
      },
    });

  expect(result1).toBe(true);

  const result2 = await db
    .open("artist")
    .is("iyaz")
    .set({
      $add: {
        age: 6,
        songCount: -30,
      },
      $delete: {
        collabs: new Set(["madonna", "akon"]),
      },
    });

  expect(result2).toBe(true);

  const result3 = await db.open("artist").is("iyaz").get(true);

  expect(result3).toHaveProperty("age", 36);
  expect(result3).toHaveProperty("songCount", 70);
  expect(result3).toHaveProperty("collabs", new Set(["charice"]));
});

test("delete artist#charice", async () => {
  const result = await db.open("artist").is("charice").remove();

  expect(result).toBe(true);
});

test("get all artist", async () => {
  const result = await db.open("artist").get();

  expect(result.items.length).toBe(2);
});

test("add album", async () => {
  const result = await db
    .open("artist")
    .is("akon")
    .open("album")
    .add("trouble", {
      year: 2003,
      songCount: 9,
    });

  expect(result?.PK).toBe("trouble");

  const result2 = await db
    .open("artist")
    .is("akon")
    .open("album")
    .add("akonda", {
      year: 2019,
      songCount: 10,
    });

  expect(result2?.PK).toBe("akonda");

  const result3 = await db
    .open("artist")
    .is("iyaz")
    .open("album")
    .add("replay", {
      year: 2010,
      songCount: 12,
    });

  expect(result3?.PK).toBe("replay");
});

test("add song", async () => {
  const troubleSong = db
    .open("artist")
    .is("akon")
    .open("album")
    .is("trouble")
    .open("song");

  db.transact([
    troubleSong.is("song3").setLater({
      name: "Kool",
      $add: {
        fever: 3,
        totalFever: 1,
      },
    }),
  ]);

  troubleSong.is("song3").set({
    name: "Kool",
    $add: {
      fever: 3,
      totalFever: 1,
    },
  });

  db.transact([
    troubleSong.is("song3").setLater({
      name: "Kool",
      $add: {
        fever: 3,
        totalFever: 1,
      },
    }),
  ]);

  const result = await db.transact([
    troubleSong.is("song3").setLater({
      name: "Kool",
      $add: {
        fever: 3,
        totalFever: 1,
      },
    }),
    troubleSong.addLater("song1", {
      title: "Song Number 1",
      genre: "pop",
      hasCollab: true,
      collabs: [
        db.open("artist").is("002").toString(),
        db.open("artist").is("001").toString(),
      ],
    }),
    troubleSong.addLater("song2", {
      title: "Song Number 1",
      genre: "pop",
    }),
  ]);

  expect(result).toBe(true);
});

test("update with condition", async () => {
  const troubleSong = db
    .open("artist")
    .is("akon")
    .open("album")
    .is("trouble")
    .open("song");

  const result = await troubleSong.is("song3").set(
    {
      name: "Kool",
      fever: 2,
    },
    [
      {
        field: "fever",
        condition: "ATTRIBUTE_TYPE",
        value: "N",
      },
    ]
  );

  expect(result).toBe(true);

  const result2 = await db.transact([
    troubleSong.is("song3").setLater(
      {
        name: "Kool Kid",
        fever: 33,
      },
      [
        {
          field: "fever",
          condition: "=",
          value: 2,
        },
      ]
    ),
  ]);

  expect(result2).toBe(true);

  const result3 = await db.transact([
    troubleSong.is("song5").setLater(
      {
        name: "Numba Gal",
        $add: {
          volumn: 2.55,
        },
      },
      [
        {
          field: "ANY",
          condition: "ATTRIBUTE_NOT_EXISTS",
        }
      ]
    ),
    troubleSong.is("song6").setLater(
      {
        name: "Goldwin",
        $add: {
          volumn: 0.45,
        },
      },
      [
        {
          field: "ANY",
          condition: "ATTRIBUTE_NOT_EXISTS",
        },
      ]
    ),
  ]);

  expect(result3).toBe(true);
});

test("conditional transaction", async () => {
  const troubleSong = db
    .open("artist")
    .is("akon")
    .open("album")
    .is("trouble")
    .open("song");

  const result = await db.transact([
    troubleSong.addLater("song4", {
      year: 2015,
      songCount: 55,
    }),
    troubleSong.is("song3").when([
      {
        field: "fever",
        condition: "BETWEEN",
        value: [30, 40],
      },
    ]),
  ]);

  expect(result).toBe(true);
});

test("add concert", async () => {
  const concertModel = db.open("artist").is("iyaz").open("concert");

  const commands = [
    concertModel.addLater(new Date(2021, 0, 16).getTime().toString(), {
      price: 200,
      attendance: 80,
    }),
    concertModel.addLater(new Date(2021, 0, 9).getTime().toString(), {
      price: 2001,
      attendance: 100,
    }),
    concertModel.addLater(new Date(2021, 0, 12).getTime().toString(), {
      price: 202,
      attendance: 100,
    }),
    concertModel.addLater(new Date(2021, 0, 23).getTime().toString(), {
      price: 200,
      attendance: 95,
    }),
    concertModel.addLater(new Date(2021, 0, 19).getTime().toString(), {
      price: 200,
      attendance: 88,
    }),
  ];

  const result = await db.transact(commands);
  expect(result).toBe(true);
});

test("get all album", async () => {
  const result = await db.open("artist").is("akon").open("album").get();
  expect(result.items.length).toBe(2);

  const result2 = await db.open("artist:album").tail().get();
  expect(result2.items.length).toBe(3);

  const key = db.key(result2.items[0][db.field("SORT")], "artist");
  expect(key).toBe("akon");
});

test("filter cache", async () => {
  const result = await db
    .open("artist:concert")
    .where("GS1SK", "=", "1610985600000")
    .get();
  expect(result.items[0].SK).toBe("artist#iyaz:concert#1610985600000");

  const result2 = await db.open("artist:album").where("year", ">", 2015).get();
  expect(result2.items.length).toBe(1);

  const result3 = await db
    .open("artist:concert")
    .where("GS1SK", "BETWEEN", [
      new Date(2021, 0, 10).getTime().toString(),
      new Date(2021, 0, 20).getTime().toString(),
    ])
    .where("attendance", "=", 100)
    .get();

  expect(result3.items[0].price).toBe(202);
});
