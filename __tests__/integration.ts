import { Collection as MongoCollection, MongoClient } from "mongodb";

import RWMutex from "../lib/RWMutex";
import { MongoLock } from "../lib/RWMutex";
const MONGO_URL = "mongodb://127.0.0.1:27017/test";
const lockID = "lockID";
const clientID = "1";

describe("Integration Test: RWMutex", () => {
  // Connect to the database
  let mongoClient: MongoClient; // only used for cleanup
  let collection: MongoCollection<MongoLock>;
  beforeAll(async () => {
    mongoClient = await MongoClient.connect(MONGO_URL);
    const db = mongoClient.db("test");
    await db.dropDatabase();
    collection = await db.createCollection("districtlocks", {
      validator: {
        $jsonSchema: {
          bsonType: "object",
          required: ["lockID", "readers", "writer"],
          properties: {
            lockID: {
              bsonType: "string",
            },
            readers: {
              bsonType: "array",
            },
            writer: {
              bsonType: "string",
            },
          },
        },
      },
    });
    await collection.createIndex("lockID", { unique: true });
  });

  // Must close the connection or jest will hang
  afterAll(() => mongoClient.close());

  // Reset the collection before each test
  beforeEach(() => collection.deleteMany({}));

  describe(".lock()", () => {
    it("inserts a lock if none exists", async () => {
      const lock = new RWMutex(collection, lockID, clientID);
      await lock.lock();

      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });
    });
  });

  describe(".unlock()", () => {
    it("throws an error if lock is not held", async () => {
      const lock = new RWMutex(collection, lockID, clientID);
      try {
        await lock.unlock();
      } catch (err) {
        return expect(err.message).toEqual(
          "lock lockID not currently held by client: 1"
        );
      }
      throw new Error("expected error to be thrown");
    });
  });
});
