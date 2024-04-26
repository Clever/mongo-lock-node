import RWMutex, { emptyReadersQuery, emptyWriterQuery } from "../lib/RWMutex";
import MockCollection from "../__mocks__/MockCollection";
import { MongoError } from "mongodb";

// ---------- Defaults ----------
const lockID = "lockID";
const clientID = "1";

// ---------- Tests ----------
describe("RWMutex", () => {
  describe(".lock()", () => {
    it("acquires the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.lock();
      const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
      writerQuery["$or"].push({ writer: clientID });
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID: lockID,
          $and: [emptyReadersQuery, writerQuery],
        },
        {
          $set: {
            writer: clientID,
            readers: [],
          },
        },
        { upsert: true },
      );
    });

    it("inserts a lock if none exists", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      mockCollection.updateOne = jest.fn().mockReturnValue(
        Promise.resolve({
          upsertedCount: 1,
        }),
      );
      await lock.lock();
      const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
      writerQuery["$or"].push({ writer: clientID });
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID: lockID,
          $and: [emptyReadersQuery, writerQuery],
        },
        {
          $set: {
            writer: clientID,
            readers: [],
          },
        },
        { upsert: true },
      );
    });

    it("waits if the lock is already in use", async () => {
      const mockCollection = new MockCollection();
      const err = new MongoError("E11000 duplicate key error collection");
      err.code = 11000;
      mockCollection.updateOne = jest
        .fn()
        .mockRejectedValueOnce(err)
        .mockReturnValueOnce(Promise.resolve({ upsertedCount: 1 }));
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      await lock.lock();
      const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
      writerQuery["$or"].push({ writer: clientID });
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(2);
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID: lockID,
          $and: [emptyReadersQuery, writerQuery],
        },
        {
          $set: {
            writer: clientID,
            readers: [],
          },
        },
        { upsert: true },
      );
    });

    it("re-enters the lock if the client id already has the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      mockCollection.updateOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ matchedCount: 1 }));
      await lock.lock();
      const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
      writerQuery["$or"].push({ writer: clientID });
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID: lockID,
          $and: [emptyReadersQuery, writerQuery],
        },
        {
          $set: {
            writer: clientID,
            readers: [],
          },
        },
        { upsert: true },
      );
    });
  });

  describe(".unlock()", () => {
    it("releases the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      mockCollection.deleteOne = jest.fn().mockReturnValue(Promise.resolve({ deletedCount: 1 }));
      await lock.unlock();
      expect(mockCollection.deleteOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.deleteOne).toHaveBeenCalledWith({
        lockID,
        writer: clientID,
        $or: emptyReadersQuery["$or"],
      });
    });

    it("returns an error if the client did not hold the lock", async () => {
      const mockCollection = new MockCollection();
      mockCollection.deleteOne = jest.fn().mockReturnValue(Promise.resolve({ deletedCount: 0 }));
      mockCollection.updateOne = jest
        .fn()
        .mockReturnValue(Promise.resolve({ matchedCount: 0, upsertedCount: 0 }));
      const lock = new RWMutex(mockCollection, lockID, clientID);

      try {
        await lock.unlock();
      } catch (err) {
        expect(err.message).toBe("lock lockID not currently held by client: 1");
        expect(mockCollection.deleteOne).toHaveBeenCalledTimes(1);
        expect(mockCollection.deleteOne).toHaveBeenCalledWith({
          lockID,
          writer: clientID,
          $or: emptyReadersQuery["$or"],
        });
        expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
        expect(mockCollection.updateOne).toHaveBeenCalledWith(
          {
            lockID,
            writer: clientID,
          },
          {
            $set: {
              writer: "",
            },
          },
        );
        return;
      }
      throw new Error("expected unlock to error");
    });
  });

  describe(".rLock()", () => {
    it("acquires the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 1 }));
      await lock.rLock();
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          $or: emptyWriterQuery["$or"],
        },
        {
          $set: {
            writer: "",
          },
          $addToSet: {
            readers: clientID,
          },
        },
        { upsert: true },
      );
    });

    it("inserts a lock if none exists", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      mockCollection.updateOne = jest.fn().mockReturnValue({ upsertedCount: 1 });
      await lock.rLock();
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          $or: emptyWriterQuery["$or"],
        },
        {
          $set: {
            writer: "",
          },
          $addToSet: {
            readers: clientID,
          },
        },
        { upsert: true },
      );
    });

    it("waits if the lock is already in use", async () => {
      const mockCollection = new MockCollection();
      const err = new MongoError("E11000 duplicate key error collection");
      err.code = 11000;
      mockCollection.updateOne = jest
        .fn()
        .mockRejectedValueOnce(err)
        .mockReturnValueOnce(Promise.resolve({ upsertedCount: 1 }));
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      await lock.rLock();
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(2);
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          $or: emptyWriterQuery["$or"],
        },
        {
          $set: {
            writer: "",
          },
          $addToSet: {
            readers: clientID,
          },
        },
        { upsert: true },
      );
    });

    it("re-enters the lock if the client id already has the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      mockCollection.updateOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ matchedCount: 1 }));
      await lock.rLock();

      expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          $or: emptyWriterQuery["$or"],
        },
        {
          $set: {
            writer: "",
          },
          $addToSet: {
            readers: clientID,
          },
        },
        { upsert: true },
      );
    });
  });

  describe(".rUnlock()", () => {
    it("releases the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.rUnlock();

      expect(mockCollection.deleteOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.deleteOne).toHaveBeenCalledWith({
        lockID,
        writer: "",
        readers: { $size: 1, $all: [clientID] },
      });
    });

    it("releases the lock with more than one reader", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      mockCollection.deleteOne = jest.fn().mockReturnValue(Promise.resolve({ deletedCount: 0 }));
      mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 1 }));
      await lock.rUnlock();
      expect(mockCollection.deleteOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.deleteOne).toHaveBeenCalledWith({
        lockID,
        writer: "",
        readers: { $size: 1, $all: [clientID] },
      });
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          readers: clientID,
        },
        {
          $pull: {
            readers: clientID,
          },
        },
      );
    });

    it("returns an error if the client did not hold the lock", async () => {
      const mockCollection = new MockCollection();
      mockCollection.deleteOne = jest.fn().mockReturnValue(Promise.resolve({ deletedCount: 0 }));
      mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 0 }));
      const lock = new RWMutex(mockCollection, lockID, clientID);

      try {
        await lock.rUnlock();
      } catch (err) {
        expect(err.message).toBe("lock lockID not currently held by client: 1");
        expect(mockCollection.deleteOne).toHaveBeenCalledTimes(1);
        expect(mockCollection.deleteOne).toHaveBeenCalledWith({
          lockID,
          writer: "",
          readers: { $size: 1, $all: [clientID] },
        });
        expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
        expect(mockCollection.updateOne).toHaveBeenCalledWith(
          {
            lockID,
            readers: clientID,
          },
          {
            $pull: {
              readers: clientID,
            },
          },
        );
        return;
      }
      throw new Error("expected unlock to error");
    });
  });
});
