import { WithId, MongoError, InsertOneResult, UpdateResult } from "mongodb";

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
  findOne: (filter: any) => Promise<WithId<MongoLock>>;
  insertOne: (doc: MongoLock) => Promise<InsertOneResult<MongoLock>>;
  updateOne: (filter: any, update: any) => Promise<UpdateResult<MongoLock>>;
}

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
    // provides readable error messages, no need to catch and rethrow
    const lock = await this._findOrCreateLock();

    // if this clientID already has the lock, we re-enter the lock and return
    if (lock.writer === this._clientID) {
      return;
    }

    // Loop and do the following:
    // 1. attempt to acquire the lock (must have no readers and no writer)
    // 2. if not acquired, sleep and retry
    let acquired = false;
    while (!acquired) {
      try {
        const result = await this._coll.updateOne(
          {
            lockID: this._lockID,
            readers: [],
            writer: "",
          },
          {
            $set: {
              writer: this._clientID,
            },
          },
        );
        if (result.matchedCount > 0) {
          acquired = true;
          return;
        }
      } catch (err) {
        throw new Error(`error aquiring lock ${this._lockID}: ${err.message}`);
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
    // provides readable error messages, no need to catch and rethrow
    const lock = await this._findOrCreateLock();
    if (lock.readers.indexOf(this._clientID) > -1) {
      // if this clientID is already a reader, we can re-enter the lock here
      return;
    }

    // Loop and do the following:
    // 1. attempt to acquire the lock (must have no writer)
    // 2. if not acquired, sleep and retry
    let acquired = false;
    while (!acquired) {
      // Acquire the lock
      try {
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
        );
        // We check matchedCount rather than modifiedCount here to make the lock re-enterable.
        // If the lock should not be re-enterable, or clientIDs are not unique, this
        // implemenation will break.
        // TODO: option to make it not re-enterable
        if (result.matchedCount > 0) {
          acquired = true;
          return;
        }
      } catch (err) {
        throw new Error(`error aquiring lock ${this._lockID}: ${err.message}`);
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

  /*
   * Finds or creates the resource in the mongo collection with the specified lockID
   * @param {string} - unique id of the resource
   * @return {Promise} - resolves with the lock object, rejects with the formatted error
   */
  async _findOrCreateLock(): Promise<WithId<MongoLock>> {
    let resultLock: WithId<MongoLock>;
    while (!resultLock) {
      try {
        resultLock = await this._coll.findOne({ lockID: this._lockID });
      } catch (err) {
        throw new Error(`error finding lock ${this._lockID}: ${err.message}`);
      }
      if (resultLock) {
        continue;
      }

      const lockToCreate: MongoLock = {
        lockID: this._lockID,
        readers: [],
        writer: "",
      };

      // attempt to create the lock
      try {
        await this._coll.insertOne(lockToCreate);
      } catch (err) {
        if (!(err instanceof MongoError) || err.code !== 11000) {
          throw new Error(`error creating lock ${this._lockID}: ${err.message}`);
        }
      }
      await timeoutPromise(this._options.sleepTime);
    }

    return resultLock;
  }
}
