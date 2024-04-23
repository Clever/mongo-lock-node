export default class MockCollection {
  findOne;
  insertOne;
  updateOne;

  constructor() {
    this.findOne = jest.fn(() => Promise.resolve(null));
    this.insertOne = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
    this.updateOne = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
  }
}
