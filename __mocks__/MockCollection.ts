export default class MockCollection {
  deleteOne;
  updateOne;

  constructor() {
    this.deleteOne = jest.fn(() => Promise.resolve({ deletedCount: 1 }));
    this.updateOne = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
  }
}
