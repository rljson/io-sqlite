// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { exampleTableCfg, TableCfg } from '@rljson/rljson';

import { existsSync } from 'fs';
import { rmdir } from 'fs/promises';
import { dirname } from 'path';
import { beforeEach, describe, expect, it } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';
import { SqlStandards } from '../src/sql-standards';


describe('IoSqlite', () => {
  let dbPath: string;
  let dbFilePath: string;
  let testDb: IoSqlite;

  beforeEach(async () => {
    // Delete existing database
    dbFilePath = await IoSqlite.exampleDbFilePath('io-sqlite-test');
    dbPath = dirname(dbFilePath);
    if (existsSync(dbPath)) {
      await rmdir(dbPath, { recursive: true });
    }

    // Create new database
    testDb = await IoSqlite.example('io-sqlite-test');
  });

  describe('deleteDatabase', () => {
    it('should delete the database', async () => {
      // Check if the database file exists
      expect(existsSync(dbFilePath)).toBe(true);

      // Delete the database
      await testDb.deleteDatabase();
      // Check if the database file exists
      expect(existsSync(dbFilePath)).toBe(false);
    });
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

  describe('allColumnNames', () => {
    const expectedColumnNames = [
      {
        name: '_hash',
        type: 'TEXT',
      },
      {
        name: 'a',
        type: 'TEXT',
      },
      {
        name: 'b',
        type: 'REAL',
      },
    ];

    describe('with a table name without a postfix', () => {
      it('should return all column names', async () => {
        await testDb.createTable({ tableCfg: exampleTableCfg() });
        const allColumnNames = await testDb.allColumnNames('table');
        expect(allColumnNames).toEqual(expectedColumnNames);
      });
    });

    describe('with a table name with a postfix', () => {
      it('should return all column names', async () => {
        // Execute example
        await testDb.createTable({ tableCfg: exampleTableCfg() });
        const allColumnNames = await testDb.allColumnNames(
          SqlStandards.addTablePostFix('table'),
        );
        expect(allColumnNames).toEqual(expectedColumnNames);
      });
    });
  });
});
