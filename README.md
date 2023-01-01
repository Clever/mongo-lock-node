# mongo-lock-node

Distributed lock client backed by mongo.

## Installation
```
#> npm install --save mongo-lock-node
```

## Usage
```javascript
import * as MongoClient from "mongodb";
import {RWMutex} from "mongo-lock-node";

// connect to mongo
const db = await MongoClient.connect("mongodb://localhost:27017/test");
const collection = db.collection("locks");
await collection.createIndex("lockID", { unique: true });

// create a client
const lockID = "lock_1"; // must be unique per lock
const clientID = "client_1"; // must be unique per client
const lock = new RWMutex(collection, lockID, clientID);

// acquire the write lock
try {
  await lock.lock();
} catch (err) {
  // other try/catch blocks omitted for brevity
  console.error(err.message); // => "error acquiring lock lock_1: connection interrupted"
  throw err;
}
// no other clients acquire a read or write lock while you have the write lock
console.log("doing important things...");
// release the write lock
await lock.unlock();

// acquire the read lock
await lock.rLock();
// other clients can also acquire a read lock, no clients can acquire the write lock
console.log("reading important things...");
// release the read lock
await lock.rUnlock();
```

## Gotchas
The current implementation is limited in a few ways. We may address these issues in the future but
right now you should be aware of them before using this library:

1. Re-enterable locks. RWMutex treats a clientID already existing on the lock in the db as a
   lock that this client owns. It re-enters the lock and proceeds as if you have the lock.
2. No heartbeat. Our current requirements for this project do not include heartbeats, so any
   client that does not call unlock will remain on the lock forever.
3. Manual setup. This library does not currently support setup. The collection you pass to the
   constructor must have a unique index on the `lockID` field.

## Testing
```
make test
```

## Building for local use
```
# This will compile lib/ to javascript in the dist/ folder
make build
```
test
