import { Collection as MongoCollection, MongoClient } from "mongodb";

import RWMutex from "../lib/RWMutex";
import { MongoLock } from "../lib/RWMutex";
const MONGO_URL = "mongodb://127.0.0.1:27017/test";
const lockID = "lockID";
const clientID = "1";

async function releaseLockAfterTimeout(lock: RWMutex, ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
  await lock.unlock();
}

async function releaseRLockAfterTimeout(lock: RWMutex, ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
  await lock.rUnlock();
}

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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
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

    it("locks the lock if the client already has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.lock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      await lock.lock();
      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });
    });

    it(".unlock() throws an error if lock is not held", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      try {
        await lock.unlock();
      } catch (err) {
        expect(err).toBeInstanceOf(Error);
        if (err instanceof Error) {
          return expect(err.message).toEqual("lock lockID not currently held by client: 1");
        }
      }
      throw new Error("expected error to be thrown");
    });

    it("releases the lock correctly", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.lock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      await lock.unlock();

      lockObject = await collection.findOne({ lockID });
      return expect(lockObject).toBeNull();
    });

    it("waits for the lock to be released if a writer has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.lock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      const startTime = performance.now();

      releaseLockAfterTimeout(lock, 1000);
      await lock2.lock();

      const endTime = performance.now();
      expect(endTime - startTime).toBeGreaterThanOrEqual(1000);

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "2",
      });
    });

    it("waits for the lock to be released if a reader has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.rLock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      const startTime = performance.now();

      releaseRLockAfterTimeout(lock, 1000);
      await lock2.lock();

      const endTime = performance.now();
      expect(endTime - startTime).toBeGreaterThanOrEqual(1000);

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "2",
      });
    });
  });

  describe(".rLock()", () => {
    it("acquires the lock", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.rLock();

      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });
    });

    it("acquires the lock if the client already has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.rLock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      await lock.rLock();
      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });
    });

    it("acquires the lock even if a reader already has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.rLock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      await lock2.rLock();

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: ["1", "2"],
        writer: "",
      });
    });

    it("releases the lock correctly", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.rLock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      await lock2.rLock();

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1", "2"],
        writer: "",
      });

      await lock.rUnlock();

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["2"],
        writer: "",
      });

      await lock2.rUnlock();
      lockObject = await collection.findOne({ lockID });
      return expect(lockObject).toBeNull();
    });

    it(".rUnlock() throws an error if lock is not held", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      try {
        await lock.rUnlock();
      } catch (err) {
        expect(err).toBeInstanceOf(Error);
        if (err instanceof Error) {
          return expect(err.message).toEqual("lock lockID not currently held by client: 1");
        }
      }
      throw new Error("expected error to be thrown");
    });

    it("waits for the lock to be released if a writer has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      const startTime = performance.now();

      releaseLockAfterTimeout(lock, 1000);
      await lock2.rLock();

      const endTime = performance.now();
      expect(endTime - startTime).toBeGreaterThanOrEqual(1000);

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["2"],
        writer: "",
      });
    });
  });

  describe(".overrideLockWriter()", () => {
    it("overrides the lock if a writer has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      await lock2.overrideLockWriter();

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "2",
      });
    });

    it("upserts the lock if it doesn't exist", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.overrideLockWriter(true);

      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });
    });

    it("overrides the lock if a reader has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.rLock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      await lock2.overrideLockWriter();

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "2",
      });
    });

    it("throws an error if there is no lock to override and upsert is false", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      try {
        await lock.overrideLockWriter(false);
      } catch (err) {
        expect(err).toBeInstanceOf(Error);
        if (err instanceof Error) {
          return expect(err.message).toEqual("error overriding lock lockID: lock not found");
        }
      }
      throw new Error("expected error to be thrown");
    });
  });

  describe(".conditonalOverrideLockWriter()", () => {
    const conditional = async (oldWriter: string, newWriter: string): Promise<boolean> => {
      return oldWriter < newWriter;
    };

    it("overrides the lock if the condition is met", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100 });
      const success = await lock2.conditionalOverrideLockWriter(conditional);
      expect(success).toBe(true);

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "2",
      });
    });

    it("does not override the lock if the condition is not met", async () => { 
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "0", { sleepTime: 100 });
      const success = await lock2.conditionalOverrideLockWriter(conditional);
      expect(success).toBe(false);

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });
    });

    it("upserts the lock if it doesn't exist", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      const success = await lock.conditionalOverrideLockWriter(conditional, true);
      expect(success).toBe(true);

      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      return expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });
    });

    it("returns false if there is no lock to override and upsert is false", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100 });
      const success = await lock.conditionalOverrideLockWriter(conditional, false);
      expect(success).toBe(false);
    });
  });
});
