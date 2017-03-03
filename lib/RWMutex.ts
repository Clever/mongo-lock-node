function timeoutPromise(delay) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, delay);
  });
}

/*
 * Client representation of a mutex in the database
 */
export default class RWMutex {
  _coll;
  _lockID;
  _clientID;
  _options;

  constructor(coll, lockID, clientID, options = { sleepTime: 5000 }) {
    this._coll = coll;
    // TODO: typecheck these
    this._lockID = lockID;
    this._clientID = clientID;
    this._options = options;
  }

  async lock() {
    // In a loop, do the following:
    // 1. Check clientID of the lock. If lock.clientID > this.clientID, bail
    // 2. Acquire the lock
    while (true) {
      // Find the lock
      let lock;
      try {
        // find the lock
        lock = await this._coll.find({ lockID: this._lockID }).limit(1).next();
        if (!lock) {
          // lock doesn't exist yet, so we should create it. Empty string is considered
          // lexographically < any other string, so we can use that as the zero value
          await this._coll.insert({
            lockID: this._lockID,
            readers: [],
            writer: "",
          });
          continue;
        }
      } catch (err) {
        throw new Error(`error finding lock: ${err.message}`);
      }

      // if this clientID already has the lock, we re-enter the lock and return
      if (lock.writer === this._clientID) {
        return;
      }

      // Acquire the lock
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
          return true;
        }
      } catch (err) {
        throw new Error(`error aquiring lock: ${err.message}`);
      }

      await timeoutPromise(this._options.sleepTime);
    }
  }

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

  async rLock() {
    while (true) {
      let lock;
      try {
        // find the lock
        lock = await this._coll.find({ lockID: this._lockID }).limit(1).next();
        if (!lock) {
          // lock doesn't exist yet, so we should create it. Empty string is considered
          // lexographically < any other string, so we can use that as the zero value
          await this._coll.insert({
            lastWriter: "",
            lockID: this._lockID,
            readers: [],
            writer: "",
          });
          continue;
        }
      } catch (err) {
        throw new Error(`error finding lock: ${err.message}`);
      }

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
        // TODO: allow configurable lock to make it not re-enterable
        if (result.matchedCount > 0) {
          return;
        }
      } catch (err) {
        throw new Error(`error aquiring lock: ${err.message}`);
      }

      await timeoutPromise(this._options.sleepTime);
    }
  }

  async rUnlock() {
    let result;
    try {
      result = await this._coll.updateOne({
        lockID: this._lockID,
        readers: this._clientID,
      }, {
        $pullAll: {
          readers: [this._clientID],
        }
      });
    } catch (err) {
      throw new Error(`error releasing lock: ${err.message}`);
    }
    if (result.matchedCount === 0) {
      throw new Error(`lock not currently held by client: ${this._clientID}`);
    }
    return;
  }
}
