// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoTestSetup } from '@rljson/io';

import { IoSqlite } from '../src/io-sqlite';

// ..............................................................................
class MyIoTestSetup implements IoTestSetup {
  async init(): Promise<void> {
    const sqlite = await IoSqlite.example();
    this._io = sqlite;
  }

  async tearDown(): Promise<void> {
    (this.io as IoSqlite).deleteDatabase();
  }

  get io(): Io {
    if (!this._io) {
      throw new Error('Call init() before accessing io');
    }
    return this._io;
  }

  private _io: Io | null = null;
}

// .............................................................................
export const testSetup = () => new MyIoTestSetup();
