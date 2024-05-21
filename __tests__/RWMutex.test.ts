import RWMutex, { emptyReadersQuery, emptyWriterQuery } from "../lib/RWMutex";
import MockCollection from "../__mocks__/MockCollection";
import { MongoError } from "mongodb";

// ---------- Defaults ----------
const lockID = "lockID";
const clientID = "1";

afterAll(() => {
  jest.restoreAllMocks();
});

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
        expiresAt: null,
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
        expiresAt: null,
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
        expect(err).toBeInstanceOf(Error);
        if (err instanceof Error) { 
          expect(err.message).toBe("lock lockID not currently held by client: 1");
        }
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
        expiresAt: null,
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
        expiresAt: null,
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
        $or: emptyWriterQuery["$or"],
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
        $or: emptyWriterQuery["$or"],
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
        expect(err).toBeInstanceOf(Error);
        if (err instanceof Error) {
          expect(err.message).toBe("lock lockID not currently held by client: 1");
        }
        expect(mockCollection.deleteOne).toHaveBeenCalledTimes(1);
        expect(mockCollection.deleteOne).toHaveBeenCalledWith({
          lockID,
          $or: emptyWriterQuery["$or"],
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
describe(".tryOverrideLockWriter()", () => {
  it("overrides the current writer of the lock", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 1, upsertedCount: 0 }));
    await lock.tryOverrideLockWriter("oldClientID", false);
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "oldClientID" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: clientID,
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: false },
    );
  });

  it("creates a new lock if one does not exist", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 0, upsertedCount: 1 }));
    await lock.tryOverrideLockWriter("oldClientID", true);
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "oldClientID" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: clientID,
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
  });

  it("throws an error if the lock is not found and upsert is false", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 0, upsertedCount: 0}));
    await expect(lock.tryOverrideLockWriter("oldClientID", false)).rejects.toThrow(
      `error overriding lock ${lockID}: lock not found`,
    );
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "oldClientID" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: clientID,
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: false },
    );
  });

  it("throws an error if the lock is not found and upsert fails", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 0, upsertedCount: 0}));
    await expect(lock.tryOverrideLockWriter("oldClientID", true)).rejects.toThrow(
      `error overriding lock ${lockID}: lock not found, upsert failed`,
    );
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "oldClientID" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: clientID,
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
  });
});
describe(".conditionalOverrideLockWriter()", () => {
  it("overrides the current writer of the lock if the conditional function returns true", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.findOne = jest.fn().mockReturnValue(Promise.resolve({ writer: "oldWriter" }));
    mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 1 }));
    const conditional = jest.fn().mockReturnValue(Promise.resolve(true));
    const result = await lock.conditionalOverrideLockWriter(conditional);
    expect(result).toBe(true);
    expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
    expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID: lockID });
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "oldWriter" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: clientID,
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
    expect(conditional).toHaveBeenCalledTimes(1);
    expect(conditional).toHaveBeenCalledWith("oldWriter", clientID);
  });

  it("does not override the current writer of the lock if the conditional function returns false", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.findOne = jest.fn().mockReturnValue(Promise.resolve({ writer: "oldWriter" }));
    const conditional = jest.fn().mockReturnValue(Promise.resolve(false));
    const result = await lock.conditionalOverrideLockWriter(conditional);
    expect(result).toBe(false);
    expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
    expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID: lockID });
    expect(mockCollection.updateOne).not.toHaveBeenCalled();
    expect(conditional).toHaveBeenCalledTimes(1);
    expect(conditional).toHaveBeenCalledWith("oldWriter", clientID);
  });

  it("creates a new lock if one does not exist and upsert is true", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.findOne = jest.fn().mockReturnValue(Promise.resolve(null));
    mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ upsertedCount: 1 }));
    const conditional = jest.fn().mockReturnValue(Promise.resolve(true));
    const result = await lock.conditionalOverrideLockWriter(conditional, true);
    expect(result).toBe(true);
    expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
    expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID: lockID });
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: clientID,
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
    expect(conditional).not.toHaveBeenCalled();
  });

  it("does not create a new lock if one does not exist and upsert is false", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.findOne = jest.fn().mockReturnValue(Promise.resolve(null));
    const conditional = jest.fn().mockReturnValue(Promise.resolve(true));
    const result = await lock.conditionalOverrideLockWriter(conditional, false);
    expect(result).toBe(false);
    expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
    expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID: lockID });
    expect(mockCollection.updateOne).not.toHaveBeenCalled();
    expect(conditional).not.toHaveBeenCalled();
  });

  it("throws an error if the lock is not found and upsert fails", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, clientID);
    mockCollection.findOne = jest.fn().mockReturnValue(Promise.resolve(null));
    mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ upsertedCount: 0 }));
    const conditional = jest.fn().mockReturnValue(Promise.resolve(true));
    await expect(lock.conditionalOverrideLockWriter(conditional, true)).rejects.toThrow(
      `error overriding lock ${lockID}: lock not found, upsert failed`,
    );
    expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
    expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID: lockID });
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: clientID,
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
    expect(conditional).not.toHaveBeenCalled();
  });

  it("reattempts to override the lock if the writer changed during the conditional function", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, "3newWriter");
    mockCollection.findOne = jest
      .fn()
      .mockReturnValueOnce(Promise.resolve({ writer: "1oldWriter" }))
      .mockReturnValueOnce(Promise.resolve({ writer: "2oldWriter" }));
    const err = new MongoError("E11000 duplicate key error collection");
    err.code = 11000;
    mockCollection.updateOne = jest.fn().
      mockRejectedValueOnce(err).
      mockReturnValueOnce(Promise.resolve({ matchedCount: 1 }));
    const conditional = jest.fn().mockReturnValue(Promise.resolve(true));
    const result = await lock.conditionalOverrideLockWriter(conditional);
    expect(result).toBe(true);
    expect(mockCollection.findOne).toHaveBeenCalledTimes(2);
    expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID: lockID });
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(2);
    let writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "1oldWriter" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: "3newWriter",
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
    writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "2oldWriter" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: "3newWriter",
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
    expect(conditional).toHaveBeenCalledTimes(2);
    expect(conditional).toHaveBeenCalledWith("1oldWriter", "3newWriter");
    expect(conditional).toHaveBeenCalledWith("2oldWriter", "3newWriter");
  });

  it("reattempts to override the lock if the writer changed during the conditional function and fails if the conditional is no longer true", async () => {
    const mockCollection = new MockCollection();
    const lock = new RWMutex(mockCollection, lockID, "3newWriter");
    mockCollection.findOne = jest
      .fn()
      .mockReturnValueOnce(Promise.resolve({ writer: "1oldWriter" }))
      .mockReturnValueOnce(Promise.resolve({ writer: "2oldWriter" }));
    const err = new MongoError("E11000 duplicate key error collection");
    err.code = 11000;
    mockCollection.updateOne = jest.fn().mockRejectedValueOnce(err);
    const conditional = jest.fn().mockReturnValueOnce(Promise.resolve(true)).
      mockReturnValueOnce(Promise.resolve(false));
    const result = await lock.conditionalOverrideLockWriter(conditional);
    expect(result).toBe(false);
    expect(mockCollection.findOne).toHaveBeenCalledTimes(2);
    expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID: lockID });
    expect(mockCollection.updateOne).toHaveBeenCalledTimes(1);
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: "1oldWriter" });
    expect(mockCollection.updateOne).toHaveBeenCalledWith(
      {
        lockID: lockID,
        $or: writerQuery["$or"],
      },
      {
        $set: {
          writer: "3newWriter",
        },
        $setOnInsert: {
          readers: [],
        },
      },
      { upsert: true },
    );
    expect(conditional).toHaveBeenCalledTimes(2);
    expect(conditional).toHaveBeenCalledWith("1oldWriter", "3newWriter");
    expect(conditional).toHaveBeenCalledWith("2oldWriter", "3newWriter");
  });
  
});
