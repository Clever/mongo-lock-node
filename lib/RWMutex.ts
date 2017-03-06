// Helper function that converts setTimeout to a Promise
function timeoutPromise(delay) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, delay);
  });
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
  _coll;
  _lockID;
  _clientID;
  _options;

  /*
   * Creates a new RWMutex
   * @param {mongodb Collection} collection - the mongodb Collection where the object should be stored
   * @param {string} lockID - id corresponding to the resource you are locking. Must be unique
   * @param {string} clientID - id corresponding to the client using this lock instance. Must be unique
   * @param {Object} options - lock options
   */
  constructor(coll, lockID, clientID, options = { sleepTime: 5000 }) {
    this._coll = coll;
    // TODO: typecheck these
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
    const lock = await this._findOrCreateLock(this._lockID);

    // if this clientID already has the lock, we re-enter the lock and return
    if (lock.writer === this._clientID) {
      return;
    }

    // Loop and do the following:
    // 1. attempt to acquire the lock (must have no readers and no writer)
    // 2. if not acquired, sleep and retry
    while (true) {
      try {
        const result = await this._coll.updateOne({
          lockID: this._lockID,
          readers: [],
          writer: "",
        }, {
          $set: {
            writer: this._clientID,
          },
        });
        if (result.matchedCount > 0) {
          return;
        }
      } catch (err) {
        throw new Error(`error aquiring lock: ${err.message}`);
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
      result = await this._coll.updateOne({
        lockID: this._lockID,
        writer: this._clientID,
      }, {
        $set: {
          writer: "",
        },
      });
    } catch (err) {
      throw new Error(`error releasing lock: ${err.message}`);
    }
    if (result.matchedCount === 0) {
      throw new Error(`lock not currently held by client: ${this._clientID}`);
    }
    return;
  }

  /*
   * Acquires the read lock.
   * @return {Promise} - Resolves when the lock is acquired, rejects if an error occurs
   */
  async rLock() {
    // provides readable error messages, no need to catch and rethrow
    const lock = await this._findOrCreateLock(this._lockID);
    if (lock.readers.indexOf(this._clientID) > -1) {
      // if this clientID is already a reader, we can re-enter the lock here
      return;
    }

    // Loop and do the following:
    // 1. attempt to acquire the lock (must have no writer)
    // 2. if not acquired, sleep and retry
    while (true) {
      // Acquire the lock
      try {
        const result = await this._coll.updateOne({
          lockID: this._lockID,
          writer: "",
        }, {
          $addToSet: {
            readers: this._clientID,
          },
        });
        // We check matchedCount rather than modifiedCount here to make the lock re-enterable.
        // If the lock should not be re-enterable, or clientIDs are not unique, this
        // implemenation will break.
        // TODO: option to make it not re-enterable
        if (result.matchedCount > 0) {
          return;
        }
      } catch (err) {
        throw new Error(`error aquiring lock: ${err.message}`);
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
      result = await this._coll.updateOne({
        lockID: this._lockID,
        readers: this._clientID,
      }, {
        $pull: {
          readers: this._clientID,
        },
      });
    } catch (err) {
      throw new Error(`error releasing lock: ${err.message}`);
    }
    if (result.matchedCount === 0) {
      throw new Error(`lock not currently held by client: ${this._clientID}`);
    }
    return;
  }

  /*
   * Finds or creates the resource in the mongo collection with the specified lockID
   * @param {string} - unique id of the resource
   * @return {Promise} - resolves with the lock object, rejects with the formatted error
   */
  async _findOrCreateLock(lockID) {
    let lock;
    try {
      lock = await this._coll.find({ lockID: this._lockID }).limit(1).next();
    } catch (err) {
      throw new Error(`error finding lock ${lockID}: ${err.message}`);
    }

    if (!lock) {
      // lock doesn't exist yet, so we should create it. Empty string is considered
      // lexographically < any other string, so we can use that as the zero value
      lock = {
        lastWriter: "",
        lockID,
        readers: [],
        writer: "",
      };
      try {
        await this._coll.insert(lock);
      } catch (err) {
        // Check for E11000 duplicate key error, which means someone inserted the lock before us
        if (err.code === 11000) {
          // set the lock to null
          lock = null;
        } else {
          throw new Error(`error creating lock for ${lockID}: ${err.message}`);
        }
      }
    }

    if (!lock) {
      // handle the duplicate key error, lock must now exist
      try {
        lock = await this._coll.find({ lockID: this._lockID }).limit(1).next();
      } catch (err) {
        throw new Error(`error finding existing lock ${lockID}: ${err.message}`);
      }
    }

    if (!lock) {
      // this should never happen
      throw new Error(`error finding and creating lock ${lockID}`);
    }
    return lock;
  }
}
