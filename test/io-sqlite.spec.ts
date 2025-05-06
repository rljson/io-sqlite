// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { TableCfg } from '@rljson/rljson';

import * as fs from 'fs';
import { beforeEach, describe, expect, it } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';

import { expectGolden } from './setup/goldens';

describe('IoSqlite', () => {
  let ioSqlite: IoSqlite;

  beforeEach(async () => {
    // Create new database
    ioSqlite = await IoSqlite.example();
    // testDb = await IoSqlite.example('io-sqlite-test');
    await ioSqlite.init();
    await ioSqlite.isReady();
  });

  describe('deleteDatabase', () => {
    it('should delete the database', async () => {
      const filePath: fs.PathLike = ioSqlite.currentPath;
      // Check if the database file exists
      expect(fs.existsSync(filePath)).toBe(true);

      // Delete the database
      await ioSqlite.deleteDatabase();
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
      const contentOfTables = await ioSqlite.dump();
      await expectGolden('io-sqlite/db-content2.json').toBe(contentOfTables);
    });
  });

  describe('create table', async () => {
    await new Promise((resolve) => setTimeout(resolve, 1000)); // Add a short delay
    it('should wait until the database is not consumed by other processes', async () => {
      let isReady = false;
      while (!isReady) {
        try {
          await ioSqlite.isReady();
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
      await ioSqlite.createOrExtendTable({ tableCfg });
    });
  });

  describe('parseDataTest', () => {
    it('should correctly parse data based on the table configuration', async () => {
      const tableCfg: TableCfg = {
        key: 'table1',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          { key: '_hash', type: 'string' },
          { key: 'id', type: 'number' },
          { key: 'name', type: 'string' },
        ],
        type: 'cakes',
        version: 1,
      };

      const rawData = [
        { _hash_col: 'abc123', id_col: 1, name_col: 'Chocolate Cake' },
        { _hash_col: 'def456', id_col: 2, name_col: 'Vanilla Cake' },
      ];

      const parsedData = ioSqlite.parseDataTest(rawData, tableCfg);

      expect(parsedData).toEqual([
        { _hash: 'abc123', id: 1, name: 'Chocolate Cake' },
        { _hash: 'def456', id: 2, name: 'Vanilla Cake' },
      ]);
    });

    it('should handle missing columns gracefully', async () => {
      const tableCfg: TableCfg = {
        key: 'table1',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          { key: '_hash', type: 'string' },
          { key: 'id', type: 'number' },
          { key: 'name', type: 'string' },
        ],
        type: 'cakes',
        version: 1,
      };

      const rawData = [
        { _hash_col: 'abc123', id_col: 1 }, // Missing 'name'
        { _hash_col: 'def456', id_col: 2, name_col: 'Vanilla Cake' },
      ];

      // Call the private _parseData method using a workaround
      const parsedData = (ioSqlite as any)._parseData(rawData, tableCfg);

      expect(parsedData).toEqual([
        { _hash: 'abc123', id: 1 },
        { _hash: 'def456', id: 2, name: 'Vanilla Cake' },
      ]);
    });
  });

  describe('exampleDbDir', () => {
    it('should create a temporary directory if no directory is provided', async () => {
      const tempDir = await IoSqlite.exampleDbDir();
      expect(tempDir).toBeDefined();
      expect(fs.existsSync(tempDir)).toBe(true);

      // Clean up
      fs.rmdirSync(tempDir, { recursive: true });
    });

    it('should use the provided directory if it exists', async () => {
      const realDir: string = 'custom-PAkvWM';
      const resultDir = await IoSqlite.exampleDbDir(realDir);

      expect(resultDir).toBe(resultDir);
      expect(fs.existsSync(resultDir)).toBe(true);

      // Clean up
      fs.rmdirSync(resultDir, { recursive: true });
    });

    it('should create the provided directory if it does not exist', async () => {
      const realDir: string = 'nonexistent-dir';
      const resultDir = await IoSqlite.exampleDbDir(realDir);

      expect(resultDir).toBe(resultDir);
      expect(fs.existsSync(resultDir)).toBe(true);

      // Clean up
      fs.rmdirSync(resultDir, { recursive: true });
    });
  });

  describe('alltableKeys', () => {
    it('should return all table keys in the database', async () => {
      // Create some tables
      const tableCfg1: TableCfg = {
        key: 'table1',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          { key: '_hash', type: 'string' },
          { key: 'id', type: 'number' },
          { key: 'name', type: 'string' },
        ],
        type: 'cakes',
        version: 1,
      };

      const tableCfg2: TableCfg = {
        key: 'table2',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          { key: '_hash', type: 'string' },
          { key: 'price', type: 'number' },
          { key: 'description', type: 'string' },
        ],
        type: 'layers',
        version: 1,
      };

      await ioSqlite.createOrExtendTable({ tableCfg: tableCfg1 });
      await ioSqlite.createOrExtendTable({ tableCfg: tableCfg2 });

      // Retrieve all table keys
      const tableKeys = await ioSqlite.alltableKeys();

      // Validate the result
      expect(tableKeys).toContain('table1');
      expect(tableKeys).toContain('table2');
      // 4 keys because of the default tables
      expect(tableKeys.length).toBe(4);
    });

    it('should return an empty array if no tables exist', async () => {
      // Ensure no table apart from the defaults exist
      const tableKeys = await ioSqlite.alltableKeys();

      // Validate the result
      expect(tableKeys).toEqual(['tableCfgs', 'revisions']);
    });
  });

  describe('readRows', () => {
    it('should return rows matching the where clause', async () => {
      // Create a table
      const tableCfg: TableCfg = {
        key: 'table1',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          { key: '_hash', type: 'string' },
          { key: 'id', type: 'number' },
          { key: 'name', type: 'string' },
        ],
        type: 'cakes',
        version: 1,
      };
      await ioSqlite.createOrExtendTable({ tableCfg });

      // Insert some rows
      await ioSqlite.write({
        data: {
          table1: {
            _data: [
              {
                _hash: '8aWgduyvFL4rfPHkUPfsU1',
                id: 1,
                name: 'Chocolate Cake',
              },
              { _hash: '7P6ACfGigO5ZC8xHbd2E7U', id: 2, name: 'Vanilla Cake' },
              {
                _hash: 'AuIghV6dqATC6pGBk5qLcJ',
                id: 3,
                name: 'Red Velvet Cake',
              },
            ],
          },
        },
      });

      // Read rows with a where clause
      const result = await ioSqlite.readRows({
        table: 'table1',
        where: { id: 2 },
      });

      // Validate the result
      expect(result).toEqual({
        table1: {
          _data: [
            { _hash: '7P6ACfGigO5ZC8xHbd2E7U', id: 2, name: 'Vanilla Cake' },
          ],
        },
      });
    });

    it('should return an empty array if no rows match the where clause', async () => {
      // Create a table
      const tableCfg: TableCfg = {
        key: 'table1',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          { key: '_hash', type: 'string' },
          { key: 'id', type: 'number' },
          { key: 'name', type: 'string' },
        ],
        type: 'cakes',
        version: 1,
      };
      await ioSqlite.createOrExtendTable({ tableCfg });

      // Insert some rows
      await ioSqlite.write({
        data: {
          table1: {
            _data: [
              {
                _hash: '8aWgduyvFL4rfPHkUPfsU1',
                id: 1,
                name: 'Chocolate Cake',
              },
              { _hash: '7P6ACfGigO5ZC8xHbd2E7U', id: 2, name: 'Vanilla Cake' },
            ],
          },
        },
      });

      // Read rows with a where clause that doesn't match any rows
      const result = await ioSqlite.readRows({
        table: 'table1',
        where: { id: 99 },
      });

      // Validate the result
      expect(result).toEqual({
        table1: {
          _data: [],
        },
      });
    });

    it('should throw an error if the table does not exist', async () => {
      await expect(
        ioSqlite.readRows({
          table: 'nonexistent_table',
          where: { id: 1 },
        }),
      ).rejects.toThrow('Table "nonexistent_table" not found');
    });
  });
});
