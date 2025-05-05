// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { TableCfg } from '@rljson/rljson';

import * as fs from 'fs';
import { writeFile } from 'fs/promises';
import { beforeEach, describe, expect, it } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';

import { expectGolden } from './setup/goldens';

describe('IoSqlite', () => {
  let testDb: IoSqlite;

  beforeEach(async () => {
    // Create new database
    testDb = await IoSqlite.example();
    // testDb = await IoSqlite.example('io-sqlite-test');
    await testDb.init();
    await testDb.isReady();
  });

  describe('deleteDatabase', () => {
    it('should delete the database', async () => {
      const filePath: fs.PathLike = testDb.currentPath;
      // Check if the database file exists
      expect(fs.existsSync(filePath)).toBe(true);

      // Delete the database
      await testDb.deleteDatabase();
      // Check if the database file exists
      expect(fs.existsSync(filePath)).toBe(false);
    });
  });

  it('should validate a io-sqlite', () => {
    const ioSqlite = IoSqlite.example;
    expect(ioSqlite).toBeDefined();
  });

  describe('creation and retrieval (timing-problem!!)', async () => {
    await new Promise((resolve) => setTimeout(resolve, 1000)); // Add a short delay
    //await testDb.isReady();
    it('should return all tables', async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Add a short delay

      // Execute example
      const contentOfTables = await testDb.dump();

      await writeFile(
        `C:/Users/Balzer/VSCode_dev/io-sqlite/test/goldens/io-sqlite/db-content2.json`,
        JSON.stringify(contentOfTables, null, 2),
      );
      await expectGolden('io-sqlite/db-content2.json').toBe(contentOfTables);
    });
  });

  describe('create table', async () => {
    await new Promise((resolve) => setTimeout(resolve, 1000)); // Add a short delay
    it('should wait until the database is not consumed by other processes', async () => {
      let isReady = false;
      while (!isReady) {
        try {
          await testDb.isReady();
          isReady = true;
        } catch (error) {
          console.log(error.message);
          await new Promise((resolve) => setTimeout(resolve, 500)); // Retry after a short delay
        }
      }
    });
    it('should create a table', async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Add a short delay
      // Execute example
      const tableCfg: TableCfg = {
        key: 'table1',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          {
            key: '_hash',
            type: 'string',
          },
          {
            key: 'id',
            type: 'number',
          },
          {
            key: 'name',
            type: 'string',
          },
        ],
        type: 'cakes',
        version: 1,
      };
      await testDb.createOrExtendTable({ tableCfg });
    });
  });
});
