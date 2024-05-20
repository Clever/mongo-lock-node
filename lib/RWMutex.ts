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
  findOne: (filter: any) => Promise<MongoLock | null>;
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
    {
      readers: null,
    },
  ],
};

export const emptyWriterQuery = {
  $or: [
    {
      writer: { $exists: false },
    },
    {
      writer: "",
    },
    {
      writer: null,
    },
  ],
};

export const DuplicateKeyErrorCode = 11000;

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
  async lock(): Promise<void> {
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
        const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
        writerQuery["$or"].push({ writer: this._clientID });
        const result = await this._coll.updateOne(
          {
            lockID: this._lockID,
            $and: [emptyReadersQuery, writerQuery],
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
        if (!(err instanceof MongoError) || err.code !== DuplicateKeyErrorCode) {
          let errMsg = `error acquiring lock ${this._lockID}`;
          if (err instanceof Error) {
            errMsg += `: ${err.message}`;
          }
          throw new Error(errMsg);
        }
      }

      await timeoutPromise(this._options.sleepTime);
    }
  }

  /*
   * Unlocks the write lock. Must have the same lock type
   * @return {Promise} - Resolves when lock is released, rejects if an error occurs
   */
  async unlock(): Promise<void> {
    let result;
    try {
      // delete lock if this is the only holder
      const deleteResult = await this._coll.deleteOne({
        lockID: this._lockID,
        writer: this._clientID,
        $or: emptyReadersQuery["$or"],
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
      let errMsg = `error releasing lock ${this._lockID}`;
      if (err instanceof Error) {
        errMsg += `: ${err.message}`;
      }
      throw new Error(errMsg);
    }
    if (result.matchedCount === 0) {
      throw new Error(`lock ${this._lockID} not currently held by client: ${this._clientID}`);
    }
    return;
  }

  overrideWriterSwitchedError = "lock already held by another writer";
  /**
   * overrideLockWriter is a method that will override the current writer of the lock with the
   * clientID of the current instance of the RWMutex.
   * @param upsert determines whether or not to create a new lock if one does not exist
   * @returns void
   */
  async tryOverrideLockWriter(oldWriter: string, upsert = false): Promise<void> {
    let errMsg = `error overriding lock ${this._lockID}`;
    const writerQuery = JSON.parse(JSON.stringify(emptyWriterQuery));
    writerQuery["$or"].push({ writer: oldWriter });
    try { 
      const result = await this._coll.updateOne(
        {
          lockID: this._lockID,
          $or: writerQuery["$or"],
        },
        {
          $set: {
            writer: this._clientID,
          },
          $setOnInsert: {
            readers: [],
          },
        },
        { upsert: upsert },
      );
      if (result.matchedCount > 0 || result.upsertedCount > 0) {
        return;
      }
    } catch (err: unknown) {
      if (err instanceof MongoError && err.code === DuplicateKeyErrorCode) { 
        errMsg += ": " + this.overrideWriterSwitchedError;
        throw new Error(errMsg);
      }
      if (err instanceof Error) {
        errMsg += `: ${err.message}`;
      }
      throw new Error(errMsg);
    }
    if (!upsert) {
      errMsg += ": lock not found";
      throw new Error(errMsg);
    }
    errMsg += ": lock not found, upsert failed";
    throw new Error(errMsg);
  }

  /**
   * conditionalOverrideLockWriter is a method that will override the current writer of the lock with the 
   * clientID of the current instance of the RWMutex if the conditional function returns true.
   * @param conditional returns a boolean value based on some comparison of the current lockID in the database and
   * the lockID of the current instance of the RWMutex
   * @param upsert determines whether or not to create a new lock if one does not exist
   * @returns boolean value based on whether or not we overrode the lock
   */
  async conditionalOverrideLockWriter(
    conditional: (oldWriter: string, newWriter: string) => Promise<boolean>,
    upsert = true, timeout = 10000): Promise<boolean> { 
    const start = Date.now();
    
    while (Date.now() - start < timeout) {
      const mongoLock = await this._coll.findOne({ lockID: this._lockID });
      if (!mongoLock) {
        if (!upsert) {
          return false;
        }
        try {
          await this.tryOverrideLockWriter("", upsert);
          return true;
        } catch (err) {
          if (err instanceof Error) {
            if (err.message.includes(this.overrideWriterSwitchedError)) { 
              await timeoutPromise(this._options.sleepTime);
              continue;
            }
          }
          throw err;
        }
      }
      const conditionalResult = await conditional(mongoLock.writer, this._clientID);
      if (conditionalResult) {
        try {
          await this.tryOverrideLockWriter(mongoLock.writer, upsert);
          return true;
        } catch (err) {
          if (err instanceof Error) {
            if (err.message.includes(this.overrideWriterSwitchedError)) { 
              await timeoutPromise(this._options.sleepTime);
              continue;
            }
          }
          throw err;
        }
      } else { 
        return false
      }
    }
    throw new Error("timeout for overriding lock writer exceeded");
}

  /*
   * Acquires the read lock.
   * @return {Promise} - Resolves when the lock is acquired, rejects if an error occurs
   */
  async rLock(): Promise<void> {
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
            $or: emptyWriterQuery["$or"],
          },
          {
            $set: {
              writer: "",
            },
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
        if (!(err instanceof MongoError) || err.code !== DuplicateKeyErrorCode) {
          let errMsg = `error acquiring lock ${this._lockID}`;
          if (err instanceof Error) {
            errMsg += `: ${err.message}`;
          }
          throw new Error(errMsg);
        }
      }

      await timeoutPromise(this._options.sleepTime);
    }
  }

  /*
   * Unlocks the read lock. Must have the same lock type
   * @return {Promise} - Resolves when lock is released, rejects if an error occurs
   */
  async rUnlock(): Promise<void> {
    let result;
    try {
      // delete lock if this is the only holder
      const deleteResult = await this._coll.deleteOne({
        lockID: this._lockID,
        $or: emptyWriterQuery["$or"],
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
      let errMsg = `error releasing lock ${this._lockID}`;
      if (err instanceof Error) {
        errMsg += `: ${err.message}`;
      }
      throw new Error(errMsg);
    }
    if (result.matchedCount === 0) {
      throw new Error(`lock ${this._lockID} not currently held by client: ${this._clientID}`);
    }
    return;
  }
}
