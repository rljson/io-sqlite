// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { TableCfg } from '@rljson/rljson';

import { describe, expect, it } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';


describe('IoSqlite', () => {
  it('should validate a io-sqlite', () => {
    const ioSqlite = IoSqlite.example;
    expect(ioSqlite).toBeDefined();
  });
});

describe('create main table', () => {
  it('should return all tables', async () => {
    // Execute example

    const testDB = await IoSqlite.example();
    const contentOfTables = await testDB.dump();
    console.log(contentOfTables);

    await testDB.deleteDatabase();
  });
});

describe('create table', () => {
  it('should create a table', async () => {
    // Execute example

    const testDB = await IoSqlite.example();
    const tableCfg: TableCfg = {
      key: 'table1',
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
    await testDB.createTable({ tableCfg });

    await testDB.deleteDatabase();
  });
});
