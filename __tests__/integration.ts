import * as MongoClient from "mongodb";

import RWMutex from "../lib/RWMutex";

const MONGO_URL = "mongodb://127.0.0.1:27017/test";
const lockID = "lockID";
const clientID = "1";

describe("RWMutex", () => {
  // Connect to the database
  let collection = null;
  beforeAll(async () => {
    const db = await MongoClient.connect(MONGO_URL);
    collection = db.collection("lock_test");
  });

  // Reset the collection before each test
  beforeEach(async () => {
    await collection.drop();
    await collection.createIndex("lockID", { unique: true });
  });

  describe(".lock()", () => {
    it("inserts a lock if none exists", async () => {
      const lock = new RWMutex(collection, lockID, clientID);
      await lock.lock();

      const lockObject = await collection.find({ lockID }).limit(1).next();
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lastWriter: "",
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
        return expect(err.message).toEqual("lock not currently held by client: 1");
      }
      throw new Error("expected error to be thrown");
    });
  });
});
