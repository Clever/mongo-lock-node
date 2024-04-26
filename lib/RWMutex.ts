import { MongoError, UpdateResult, DeleteResult, UpdateOptions } from "mongodb";

// Helper function that converts setTimeout to a Promise
function timeoutPromise(delay) {
  return new Promise((resolve) => {
    setTimeout(resolve, delay);
  });
}

export interface MongoLock {
  lockID: string;
  readers: string[];
  writer: string;
}

export interface MongoLockCollection {
  deleteOne: (filter: any) => Promise<DeleteResult>;
  updateOne: (filter: any, update: any, opts?: UpdateOptions) => Promise<UpdateResult<MongoLock>>;
}

export const emptyReadersQuery = {
  $or: [
    {
      readers: { $size: 0 },
    },
    {
      readers: { $exists: false },
    },
  ],
};

/*
 * RWMutex implements a distributed reader/writer lock backed by mongodb. Right now it is limited
 * in a few key ways:
 * 1. Re-enterable locks. RWMutex treats a clientID already existing on the lock in the db as a
 *    lock that this client owns. It re-enters the lock and proceeds as if you have the lock.
 * 2. No heartbeat. Our current requirements for this project do not include heartbeats, so any
 *    client that does not call unlock will remain on the lock forever.
 * 3. Manual setup. This library does not currently support setup. The collection you pass to the
 *    constructor must have a unique index on the `lockID` field.
 */
export default class RWMutex {
  _coll: MongoLockCollection;
  _lockID: string;
  _clientID: string;
  _options: { sleepTime: number };

  /*
   * Creates a new RWMutex
   * @param {mongodb Collection} collection - the mongodb Collection where the object should be stored
   * @param {string} lockID - id corresponding to the resource you are locking. Must be unique
   * @param {string} clientID - id corresponding to the client using this lock instance. Must be unique
   * @param {Object} options - lock options
   */
  constructor(
    coll: MongoLockCollection,
    lockID: string,
    clientID: string,
    options = { sleepTime: 1000 },
  ) {
    this._coll = coll;
    this._lockID = lockID;
    this._clientID = clientID;
    this._options = options;
  }

  /*
   * Acquires the write lock.
   * @return {Promise} - Promise that resolves when the lock is acquired, rejects if an error occurs
   */
  async lock() {
    // Loop and do the following:
    // 1. attempt to acquire the lock (must have no readers and no writer)
    // 2. if not acquired, sleep and retry
    let acquired = false;
    while (!acquired) {
      try {
        // If no such lock exists, this will create it
        // If a lock exists with this lockID with no readers and no writer, this will update it
        // If a lock exists with this lockID with clientID as the writer and no readers,
        // this will do nothing
        // If a lock exists with this lockID with a different clientID as the writer or readers,
        // this will throw an error which will be caught.  We will then retry.
        const result = await this._coll.updateOne(
          {
            lockID: this._lockID,
            $and: [
              emptyReadersQuery,
              {
                $or: [
                  {
                    writer: "",
                  },
                  {
                    writer: this._clientID,
                  },
                ],
              },
            ],
          },
          {
            $set: {
              writer: this._clientID,
              readers: [],
            },
          },
          { upsert: true },
        );
        if (result.matchedCount > 0 || result.upsertedCount > 0) {
          acquired = true;
          return;
        }
      } catch (err) {
        if (!(err instanceof MongoError) || err.code !== 11000) {
          throw new Error(`error aquiring lock ${this._lockID}: ${err.message}`);
        }
      }

      await timeoutPromise(this._options.sleepTime);
    }
  }

  /*
   * Unlocks the write lock. Must have the same lock type
   * @return {Promise} - Resolves when lock is released, rejects if an error occurs
   */
  async unlock() {
    let result;
    try {
      // delete lock if this is the only holder
      const deleteResult = await this._coll.deleteOne({
        lockID: this._lockID,
        writer: this._clientID,
        readers: [],
      });
      if (deleteResult.deletedCount > 0) {
        return;
      }

      // otherwise, remove the clientID from the writer field
      result = await this._coll.updateOne(
        {
          lockID: this._lockID,
          writer: this._clientID,
        },
        {
          $set: {
            writer: "",
          },
        },
      );
    } catch (err) {
      throw new Error(`error releasing lock ${this._lockID}: ${err.message}`);
    }
    if (result.matchedCount === 0) {
      throw new Error(`lock ${this._lockID} not currently held by client: ${this._clientID}`);
    }
    return;
  }

  /*
   * Acquires the read lock.
   * @return {Promise} - Resolves when the lock is acquired, rejects if an error occurs
   */
  async rLock() {
    // Loop and do the following:
    // 1. attempt to acquire the lock (must have no writer)
    // 2. if not acquired, sleep and retry
    let acquired = false;
    while (!acquired) {
      // Acquire the lock
      try {
        // If no such lock exists, this will create it
        // If a lock exists with this lockID with no writer, this will update it to add the clientID
        // to the readers list
        // If a lock exists with this lockID with a writer, this will throw an error which will be
        // caught.  We will then retry.
        const result = await this._coll.updateOne(
          {
            lockID: this._lockID,
            writer: "",
          },
          {
            $addToSet: {
              readers: this._clientID,
            },
          },
          { upsert: true },
        );
        if (result.matchedCount > 0 || result.upsertedCount > 0) {
          acquired = true;
          return;
        }
      } catch (err) {
        if (!(err instanceof MongoError) || err.code !== 11000) {
          throw new Error(`error aquiring lock ${this._lockID}: ${err.message}`);
        }
      }

      await timeoutPromise(this._options.sleepTime);
    }
  }

  /*
   * Unlocks the read lock. Must have the same lock type
   * @return {Promise} - Resolves when lock is released, rejects if an error occurs
   */
  async rUnlock() {
    let result;
    try {
      // delete lock if this is the only holder
      const deleteResult = await this._coll.deleteOne({
        lockID: this._lockID,
        writer: "",
        readers: { $size: 1, $all: [this._clientID] },
      });
      if (deleteResult.deletedCount > 0) {
        return;
      }

      // otherwise, remove the clientID from the readers list
      result = await this._coll.updateOne(
        {
          lockID: this._lockID,
          readers: this._clientID,
        },
        {
          $pull: {
            readers: this._clientID,
          },
        },
      );
    } catch (err) {
      throw new Error(`error releasing lock ${this._lockID}: ${err.message}`);
    }
    if (result.matchedCount === 0) {
      throw new Error(`lock ${this._lockID} not currently held by client: ${this._clientID}`);
    }
    return;
  }
}
