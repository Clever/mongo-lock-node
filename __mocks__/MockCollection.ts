export default class MockCollection {
  findOne;
  deleteOne;
  insertOne;
  updateOne;

  constructor() {
    this.findOne = jest.fn(() => Promise.resolve(null));
    this.deleteOne = jest.fn(() => Promise.resolve({ deletedCount: 1 }));
    this.insertOne = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
    this.updateOne = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
  }
}
