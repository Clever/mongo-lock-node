import RWMutex from "../lib/RWMutex";
import MockCollection from "../__mocks__/MockCollection";

// ---------- Defaults ----------
const lockID = "lockID";
const clientID = "1";

// ---------- Tests ----------
describe("RWMutex", () => {
  describe(".lock()", () => {
    it("acquires the lock", async () => {
      const mockCollection = new MockCollection();
      mockCollection.findOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ lockID, readers: [], writer: "" }));
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.lock();

      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          readers: [],
          writer: "",
        },
        {
          $set: {
            writer: "1",
          },
        },
      );
    });

    it("inserts a lock if none exists", async () => {
      const mockCollection = new MockCollection();
      mockCollection.findOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve(null))
        .mockReturnValueOnce(Promise.resolve({ lockID, readers: [], writer: "" }));
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.lock();

      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.insertOne).toHaveBeenCalledWith({
        lockID,
        readers: [],
        writer: "",
      });
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          readers: [],
          writer: "",
        },
        {
          $set: {
            writer: "1",
          },
        },
      );
    });

    it("waits if the lock is already in use", async () => {
      const mockCollection = new MockCollection();
      mockCollection.findOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ lockID, readers: [], writer: "different" }));
      mockCollection.updateOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ matchedCount: 0 }))
        .mockReturnValueOnce(Promise.resolve({ matchedCount: 1 }));
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      await lock.lock();

      expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(2);
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          readers: [],
          writer: "",
        },
        {
          $set: {
            writer: "1",
          },
        },
      );
    });

    it("re-enters the lock if the client id already has the lock", async () => {
      const mockCollection = new MockCollection();
      mockCollection.findOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ lockID, readers: [], writer: clientID }));
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      await lock.lock();

      expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.insertOne).toHaveBeenCalledTimes(0);
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(0);
    });
  });

  describe(".unlock()", () => {
    it("releases the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.unlock();

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
    });

    it("returns an error if the client did not hold the lock", async () => {
      const mockCollection = new MockCollection();
      mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 0 }));
      const lock = new RWMutex(mockCollection, lockID, clientID);

      try {
        await lock.unlock();
      } catch (err) {
        expect(err.message).toBe("lock lockID not currently held by client: 1");
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
      mockCollection.findOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ lockID, readers: [], writer: "" }));
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.rLock();

      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          writer: "",
        },
        {
          $addToSet: {
            readers: clientID,
          },
        },
      );
    });

    it("inserts a lock if none exists", async () => {
      const mockCollection = new MockCollection();
      mockCollection.findOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve(null))
        .mockReturnValueOnce(Promise.resolve({ lockID, readers: [], writer: "" }));
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.rLock();

      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.insertOne).toHaveBeenCalledWith({
        lockID,
        readers: [],
        writer: "",
      });
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          writer: "",
        },
        {
          $addToSet: {
            readers: clientID,
          },
        },
      );
    });

    it("waits if the lock is already in use", async () => {
      const mockCollection = new MockCollection();
      mockCollection.findOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ lockID, readers: [], writer: "different" }));
      mockCollection.updateOne = jest
        .fn()
        .mockReturnValueOnce(Promise.resolve({ matchedCount: 0 }))
        .mockReturnValueOnce(Promise.resolve({ matchedCount: 1 }));
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      await lock.rLock();

      expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(2);
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        {
          lockID,
          writer: "",
        },
        {
          $addToSet: {
            readers: clientID,
          },
        },
      );
    });

    it("re-enters the lock if the client id already has the lock", async () => {
      const mockCollection = new MockCollection();
      mockCollection.findOne = jest.fn().mockReturnValueOnce(
        Promise.resolve({
          lockID,
          readers: ["different", clientID],
          writer: "",
        }),
      );
      const lock = new RWMutex(mockCollection, lockID, clientID, {
        sleepTime: 1,
      });
      await lock.rLock();

      expect(mockCollection.findOne).toHaveBeenCalledTimes(1);
      expect(mockCollection.findOne).toHaveBeenCalledWith({ lockID });
      expect(mockCollection.insertOne).toHaveBeenCalledTimes(0);
      expect(mockCollection.updateOne).toHaveBeenCalledTimes(0);
    });
  });

  describe(".rUnlock()", () => {
    it("releases the lock", async () => {
      const mockCollection = new MockCollection();
      const lock = new RWMutex(mockCollection, lockID, clientID);
      await lock.rUnlock();

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
      mockCollection.updateOne = jest.fn().mockReturnValue(Promise.resolve({ matchedCount: 0 }));
      const lock = new RWMutex(mockCollection, lockID, clientID);

      try {
        await lock.rUnlock();
      } catch (err) {
        expect(err.message).toBe("lock lockID not currently held by client: 1");
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
