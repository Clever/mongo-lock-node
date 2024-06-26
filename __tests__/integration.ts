import { Collection as MongoCollection, MongoClient } from "mongodb";
import { RWMutex, MongoLock } from "../lib/RWMutex";
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
            expiresAt: {
              bsonType: "date",
            },
          },
        },
      },
    });
    await collection.createIndex("lockID", { unique: true });
    await collection.createIndex("expiresAt", { expireAfterSeconds: 0 });
  });

  // Must close the connection or jest will hang
  afterAll(() => mongoClient.close());

  // Reset the collection before each test
  beforeEach(() => collection.deleteMany({}));

  describe(".lock()", () => {
    it("inserts a lock if none exists", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.lock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.rLock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
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

    it("expires the lock after the expiresAt time", async () => {
      const lock = new RWMutex(
        collection,
        lockID,
        clientID,
        { sleepTime: 100, expiresAt: new Date(Date.now() + 5000) });
      await lock.lock();
      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: clientID,
      });

      await new Promise((resolve) => setTimeout(resolve, 90000));
      const expiredLock = await collection.findOne({ lockID });
      return expect(expiredLock).toBeNull();
    }, 120000);

    it("acquires the lock if the old lock has expired", async () => {
      const lock = new RWMutex(
        collection,
        lockID,
        clientID,
        { sleepTime: 100, expiresAt: new Date(Date.now() + 5000) });
      await lock.lock();

      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: clientID,
      });

      const lock2 = new RWMutex(
        collection,
        lockID,
        "2",
        { sleepTime: 100, expiresAt: null });
      lock2.lock();
      let lockObject2 = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: clientID,
      });

      await new Promise((resolve) => setTimeout(resolve, 90000));
      lockObject2 = await collection.findOne({ lockID });
      expect(lockObject2).not.toBeNull();
      delete lockObject2._id;
      return expect(lockObject2).toMatchObject({
        lockID,
        readers: [],
        writer: "2",
      });

    }, 120000);
  });

  describe(".rLock()", () => {
    it("acquires the lock", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.rLock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.rLock();

      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
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

    it("expires the lock after the expiresAt time", async () => {
      const lock = new RWMutex(
        collection,
        lockID,
        clientID,
        { sleepTime: 100, expiresAt: new Date(Date.now() + 5000) });
      await lock.rLock();
      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [clientID],
        writer: "",
      });

      await new Promise((resolve) => setTimeout(resolve, 90000));
      const expiredLock = await collection.findOne({ lockID });
      return expect(expiredLock).toBeNull();
    }, 120000);

    it("acquires the lock if the old lock has expired", async () => {
      const lock = new RWMutex(
        collection,
        lockID,
        clientID,
        { sleepTime: 100, expiresAt: new Date(Date.now() + 5000) });
      await lock.rLock();

      const lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [clientID],
        writer: "",
      });

      const lock2 = new RWMutex(
        collection,
        lockID,
        "2",
        { sleepTime: 100, expiresAt: null });
      lock2.lock();
      let lockObject2 = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [clientID],
        writer: "",
      });

      await new Promise((resolve) => setTimeout(resolve, 90000));
      lockObject2 = await collection.findOne({ lockID });
      expect(lockObject2).not.toBeNull();
      delete lockObject2._id;
      return expect(lockObject2).toMatchObject({
        lockID,
        readers: [],
        writer: "2",
      });

    }, 120000);    
  });

  describe(".tryOverrideLockWriter()", () => {
    it("overrides the lock if a writer has it", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
      await lock2.tryOverrideLockWriter(clientID);

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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.tryOverrideLockWriter(clientID, true);

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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.rLock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: ["1"],
        writer: "",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
      await lock2.tryOverrideLockWriter("");

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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      try {
        await lock.tryOverrideLockWriter("", false);
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
    let conditional: (oldWriter: string, newWriter: string) => Promise<boolean>;
    beforeEach(() => {
      conditional = async (oldWriter: string, newWriter: string): Promise<boolean> => {
        return oldWriter < newWriter;
      };
    });

    it("overrides the lock if the condition is met", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      const lock2 = new RWMutex(collection, lockID, "0", { sleepTime: 100, expiresAt: null });
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

    it("reattempts to override the lock if the writer changed", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      // to simulate a race condition where the writer changes we will sleep for 3 seconds
      // before returning the result of the conditional
      conditional = async (oldWriter: string, newWriter: string): Promise<boolean> => {
        const successful = oldWriter < newWriter;
        await new Promise((resolve) => setTimeout(resolve, 3000));
        return successful;
      };

      const lock2 = new RWMutex(collection, lockID, "3", { sleepTime: 100, expiresAt: null });
      const conditionalOverridePromise = lock2.conditionalOverrideLockWriter(conditional);
      await collection.updateOne({ lockID }, { $set: { writer: "2" } });
      const success = await conditionalOverridePromise;
      expect(success).toBe(true);

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "3",
      });
    }, 10000);

    it("reattempts to override the lock if the writer changed and fails if the condition is no longer true", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      await lock.lock();
      let lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "1",
      });

      // to simulate a race condition where the writer changes we will sleep for 3 seconds
      // before returning the result of the conditional
      conditional = async (oldWriter: string, newWriter: string): Promise<boolean> => {
        const successful = oldWriter < newWriter;
        await new Promise((resolve) => setTimeout(resolve, 3000));
        return successful;
      };

      const lock2 = new RWMutex(collection, lockID, "2", { sleepTime: 100, expiresAt: null });
      const conditionalOverridePromise = lock2.conditionalOverrideLockWriter(conditional);
      await collection.updateOne({ lockID }, { $set: { writer: "3" } });
      const success = await conditionalOverridePromise;
      expect(success).toBe(false);

      lockObject = await collection.findOne({ lockID });
      expect(lockObject).not.toBeNull();
      delete lockObject._id;
      expect(lockObject).toMatchObject({
        lockID,
        readers: [],
        writer: "3",
      });
    }, 10000);

    it("upserts the lock if it doesn't exist", async () => {
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
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
      const lock = new RWMutex(collection, lockID, clientID, { sleepTime: 100, expiresAt: null });
      const success = await lock.conditionalOverrideLockWriter(conditional, false);
      expect(success).toBe(false);
    });
  });
});
