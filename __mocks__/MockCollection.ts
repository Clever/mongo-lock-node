export default class MockCollection {
  findOne;
  deleteOne;
  updateOne;

  constructor() {
    this.findOne = jest.fn(() => Promise.resolve());
    this.deleteOne = jest.fn(() => Promise.resolve({ deletedCount: 1 }));
    this.updateOne = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
  }
}
