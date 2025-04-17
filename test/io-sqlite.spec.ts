// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { TableCfg } from '@rljson/rljson';

import { existsSync } from 'fs';
import { rmdir } from 'fs/promises';
import { beforeEach, describe, expect, it } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';

describe('IoSqlite', () => {
  let testDb: IoSqlite;

  beforeEach(async () => {
    // Delete existing database
    const dbPath = await IoSqlite.exampleDbPath('io-sqlite-test');
    if (existsSync(dbPath)) {
      await rmdir(dbPath, { recursive: true });
    }

    // Create new database
    testDb = await IoSqlite.example('io-sqlite-test');
  });

  it('should validate a io-sqlite', () => {
    const ioSqlite = IoSqlite.example;
    expect(ioSqlite).toBeDefined();
  });

  describe('create main table', () => {
    it('should return all tables', async () => {
      // Execute example
      const contentOfTables = await testDb.dump();
      console.log(contentOfTables);
    });
  });

  describe('create table', () => {
    it('should create a table', async () => {
      // Execute example
      const tableCfg: TableCfg = {
        key: 'table1',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: {
          id: {
            key: 'id',
            type: 'number',
          },
          name: {
            key: 'name',
            type: 'string',
          },
        },
        type: 'cakes',
        version: 1,
      };
      await testDb.createTable({ tableCfg });
    });
  });
});
