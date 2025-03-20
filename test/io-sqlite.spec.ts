// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';

describe('IoSqlite', () => {
  it('should validate a io-sqlite', () => {
    const ioSqlite = IoSqlite.example;
    expect(ioSqlite).toBeDefined();
  });
});
