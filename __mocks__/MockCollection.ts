function cursor() {
  const j = jest.fn();
  j.limit = jest.fn(() => j);
  j.next = jest.fn(() => Promise.resolve({ lockID: "", readers: [], writer: "" }));
  return j;
}

export default class MockCollection {
  _cursor;
  find;
  insert;
  updateOne;

  constructor() {
    this._cursor = cursor();
    this.find = jest.fn(() => this._cursor);
    this.insert = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
    this.updateOne = jest.fn(() => Promise.resolve({ matchedCount: 1 }));
  }
}
