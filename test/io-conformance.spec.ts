// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, rmhsh } from '@rljson/hash';
import { exampleTableCfg, IngredientsTable, Rljson, TableCfg, TableType } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';

import { expectGolden } from './setup/goldens.ts';


describe('Io Conformance', async () => {
  let io: IoSqlite;

  beforeEach(async () => {
    io = await IoSqlite.example();
    await io.isReady();
  });

  describe('isReady()', () => {
    it('should return a resolved promise', async () => {
      await io.isReady();
    });
  });

  const createExampleTable = async (key: string) => {
    // Register a new table config and generate the table
    const tableCfg: TableCfg = exampleTableCfg({ key });
    try {
      await io.createTable({ tableCfg: tableCfg });
    } catch (error) {
      console.error('Error creating table:', error);
      throw error; // Re-throw the error after logging it
    }
  };

  describe('tableCfgs table', () => {
    it('should be available after isReady() resolves', async () => {
      const dump = await io.dumpTable({ table: 'tableCfgs' });
      await expectGolden('io-mem/tableCfgs.json').toBe(dump);
    });
  });

  describe('allTableNames()', () => {
    it('should return an empty array if no tables are created', async () => {
      const tables = await io.allTableNames();
      expect(tables).toEqual(['tableCfgs', 'revisions']);
    });

    it('should return the names of all tables', async () => {
      await createExampleTable('table1');
      await createExampleTable('table2');

      const tables = await io.allTableNames();
      expect(tables).toEqual(['tableCfgs', 'revisions', 'table1', 'table2']);
    });
  });

  describe('tableCfgs()', () => {
    it('should return an rljson object containing the newest config for each table', async () => {
      //create four tables with two versions each
      const table0V1: TableCfg = {
        key: 'table0',
        type: 'ingredients',
        isHead: false,
        isRoot: false,
        isShared: true,
        version: 1,
        columns: {
          col0: { key: 'col0', type: 'string' },
          col1: { key: 'col1', type: 'string' },
          col2: { key: 'col2', type: 'string' },
        },
      };

      await io.createTable({ tableCfg: table0V1 });
      const table0V2 = { ...table0V1, version: 2 };
      await io.createTable({ tableCfg: table0V2 });
      const table1V1 = { ...table0V1, key: 'table1', version: 1 };
      await io.createTable({ tableCfg: table1V1 });
      const table1V2 = { ...table1V1, version: 2 };
      await io.createTable({ tableCfg: table1V2 });

      // Check the tableCfgs
      const actualTableCfgs = (await io.tableCfgs()).tableCfgs
        ._data as unknown as TableCfg[];

      expect(actualTableCfgs.length).toBe(4);
      expect((actualTableCfgs[0] as TableCfg).key).toBe('tableCfgs');
      expect((actualTableCfgs[0] as TableCfg).version).toBe(1);
      expect((actualTableCfgs[1] as TableCfg).key).toBe('revisions');
      expect((actualTableCfgs[1] as TableCfg).version).toBe(1);
      expect((actualTableCfgs[2] as TableCfg).key).toBe('table0');
      expect((actualTableCfgs[2] as TableCfg).version).toBe(2);
      expect((actualTableCfgs[3] as TableCfg).key).toBe('table1');
      expect((actualTableCfgs[3] as TableCfg).version).toBe(2);
    });
  });

  describe('throws error', async () => {
    it('if the table already exists', async () => {
      await createExampleTable('tableX');
      await expect(createExampleTable('tableX')).rejects.toThrow(
        'Table tableX already exists',
      );
    });

    it('if the hashes in the tableCfg are wrong', async () => {
      const tableCfg: TableCfg = hip(exampleTableCfg({ key: 'table1' }));
      tableCfg._hash = 'wrongHash';
      let message: string = '';
      try {
        await io.createTable({ tableCfg: tableCfg });
      } catch (err: any) {
        message = err.message;
      }

      expect(message).toBe(
        'Hash "wrongHash" does not match the newly calculated one "MkmRHAH1WplV0OSRcGaN7s". ' +
          'Please make sure that all systems are producing the same hashes.',
      );
    });
  });

  describe('createTable(request)', () => {
    it('should add a table and a table config', async () => {
      const tablesFromConfig = async () => {
        const tables = (await io.tableCfgs())
          .tableCfgs as IngredientsTable<TableCfg>;

        return tables._data.map((e) => e.key);
      };

      const physicalTables = async () => await io.allTableNames();

      // Create a first table
      await createExampleTable('table1');

      expect(await tablesFromConfig()).toEqual([
        'tableCfgs',
        'revisions',
        'table1',
      ]);
      expect(await physicalTables()).toEqual([
        'tableCfgs',
        'revisions',
        'table1',
      ]);

      // Create a second table
      await createExampleTable('table2');
      expect(await tablesFromConfig()).toEqual([
        'tableCfgs',
        'revisions',
        'table1',
        'table2',
      ]);
      expect(await physicalTables()).toEqual([
        'tableCfgs',
        'revisions',
        'table1',
        'table2',
      ]);
    });

    describe('throws', async () => {
      it('if the table already exists', async () => {
        await createExampleTable('table');
        await expect(createExampleTable('table')).rejects.toThrow(
          'Table table already exists',
        );
      });

      it('if the hashes in the tableCfg are wrong', async () => {
        const tableCfg: TableCfg = hip(exampleTableCfg({ key: 'table' }));
        const rightHash = tableCfg._hash;
        tableCfg._hash = 'wrongHash';
        let message: string = '';
        try {
          await io.createTable({ tableCfg: tableCfg });
        } catch (err: any) {
          message = err.message;
        }

        expect(message).toBe(
          `Hash "wrongHash" does not match the newly calculated one "${rightHash}". ` +
            'Please make sure that all systems are producing the same hashes.',
        );
      });

      it('if a column has an unsupported type', async () => {
        const tableCfg: TableCfg = exampleTableCfg({ key: 'table' });
        const invalidTableCfg: TableCfg = {
          ...tableCfg,
          columns: {
            ...tableCfg.columns,
            unsupportedType: {
              key: 'unsupportedType',
              type: 'unsupported' as any,
            },
          },
        };

        await expect(
          io.createTable({ tableCfg: invalidTableCfg }),
        ).rejects.toThrow('Unknown data type: unsupported');
      });
    });
  });

  describe('write(request)', async () => {
    it('adds data to existing data', async () => {
      const exampleCfg: TableCfg = exampleTableCfg({ key: 'tableA' });
      const tableCfg: TableCfg = {
        ...exampleCfg,
        columns: {
          keyA1: { key: 'keyA1', type: 'string' },
          keyA2: { key: 'keyA2', type: 'string' },
          keyB2: { key: 'keyB2', type: 'string' },
        },
      };

      await io.createTable({ tableCfg });
      const allTableNames = await io.allTableNames();
      expect(allTableNames).toContain('tableA');

      expect('tableA').toBe(tableCfg.key);

      // Write a first item
      await io.write({
        data: {
          tableA: {
            _type: 'ingredients',
            _data: [{ keyA2: 'a2' }],
          },
        },
      });

      expect(io._currentCount('tableA')).toEqual(1);

      const dump = await io.dump();
      const items = (dump.tableA as TableType)._data;
      expect(items).toEqual([
        {
          _hash: 'apLP3I2XLnVm13umIZdVhV',
          keyA1: null,
          keyA2: 'a2',
          keyB2: null,
        },
      ]);

      // Write a second item
      await io.write({
        data: {
          tableA: {
            _type: 'ingredients',
            _data: [{ keyB2: 'b2' }],
          },
        },
      });

      const dump2 = await io.dump();
      const items2 = (dump2.tableA as TableType)._data;
      expect(items2).toEqual([
        {
          _hash: 'apLP3I2XLnVm13umIZdVhV',
          keyA1: null,
          keyA2: 'a2',
          keyB2: null,
        },
        {
          _hash: 'oNNJMCE_2iycGPDyM_5_lp',
          keyA1: null,
          keyA2: null,
          keyB2: 'b2',
        },
      ]);
    });

    it('does not add the same data twice', async () => {
      const tableName = 'testTable';
      const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
      const tableCfg: TableCfg = {
        ...exampleCfg,
        columns: {
          string: { key: 'string', type: 'string' },
          number: { key: 'number', type: 'number' },
          null: { key: 'null', type: 'null' },
          boolean: { key: 'boolean', type: 'boolean' },
          array: { key: 'array', type: 'jsonArray' },
          object: { key: 'object', type: 'json' },
        },
      };

      await io.createTable({ tableCfg });
      const allTableNames = await io.allTableNames();
      expect(allTableNames).toContain(tableName);
      const allColumns = await io.allColumnNames(tableName);
      expect(allColumns.some((column) => column.name === '_hash')).toBe(true);

      const rows = [
        {
          string: 'hello',
          number: 5,
          null: null,
          boolean: 1, //true
          array: '[1, 2, { a: 10 }]',
          object: '{ a: 1, b: { c: 3 } }',
        },
        {
          string: 'world',
          number: 6,
          null: null,
          boolean: 1, //true
          array: '[1, 2, { a: 10 }]',
          object: '{ a: 1, b: 2 }',
        },
      ];

      const testData: Rljson = {
        testTable: {
          _type: 'ingredients',
          _data: rows,
        },
      };

      // Write a first item
      await expect(io.write({ data: testData })).resolves.toBeUndefined();

      // Write the same item again
      await expect(io.write({ data: testData })).rejects.toThrow(
        'Errors occurred: Error inserting into table testTable: UNIQUE constraint failed: testTable._hash, Error inserting into table testTable: UNIQUE constraint failed: testTable._hash',
      );
    });

    describe('throws', () => {
      it('when table does not exist', async () => {
        await expect(
          io.write({
            data: {
              tableA: {
                _type: 'ingredients',
                _data: [{ keyA2: 'a2' }],
              },
            },
          }),
        ).rejects.toThrow('Table tableA does not exist');
      });

      it('when the table has a different type then an existing one', async () => {
        const exampleCfg: TableCfg = exampleTableCfg({ key: 'tableA' });
        const tableCfg: TableCfg = {
          ...exampleCfg,
          columns: {
            col0: { key: 'keyA1', type: 'string' },
            col1: { key: 'keyA2', type: 'string' },
            col2: { key: 'keyB2', type: 'string' },
          },
        };
        await io.createTable({ tableCfg });

        await expect(
          io.write({
            data: {
              tableA: {
                _type: 'cakes',
                _data: [
                  {
                    keyB2: 'b2',
                    sliceIdsRow: 'xyz',
                    sliceIdsTable: 'xyz',
                    itemIds2: 'xyz',
                    layersTable: 'xyz',
                    layers: {},
                    collections: 'xyz',
                  },
                ],
              },
            },
          }),
        ).rejects.toThrow(
          'Errors occurred: Table type check failed for table tableA, cakes',
        );
      });
    });
  });

  describe('readRows({table, where})', async () => {
    describe('should return rows matching the where clause', async () => {
      beforeEach(async () => {
        const tableName = 'testTable';
        const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
        const tableCfg: TableCfg = {
          ...exampleCfg,
          columns: {
            string: { key: 'string', type: 'string' },
            number: { key: 'number', type: 'number' },
            null: { key: 'null', type: 'null' },
            boolean: { key: 'boolean', type: 'boolean' },
            array: { key: 'array', type: 'jsonArray' },
            object: { key: 'object', type: 'json' },
          },
        };
        await io.createTable({ tableCfg });

        const testData: Rljson = {
          testTable: {
            _type: 'ingredients',
            _data: [
              {
                string: 'hello',
                number: 5,
                null: null,
                boolean: 1,
                array: [1, 2, { a: 10 }],
                object: { a: 1, b: { c: 3 } },
              },
              {
                string: 'world',
                number: 6,
                null: null,
                boolean: 1,
                array: [1, 2, { a: 10 }],
                object: { a: 1, b: 2 },
              },
            ],
          },
        };
        await io.write({ data: testData });
        console.log('Data written to the table successfully.');
      });

      it('with where searching string values', async () => {
        const result = rmhsh(
          await io.readRows({
            table: 'testTable',
            where: { string: 'hello' },
          }),
        );

        expect(result).toEqual({
          testTable: {
            _data: [
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 5,
                object: {
                  a: 1,
                  b: {
                    c: 3,
                  },
                },
                string: 'hello',
              },
            ],
          },
        });
      });

      it('with where searching number values', async () => {
        const result = rmhsh(
          await io.readRows({
            table: 'testTable',
            where: { number: 6 },
          }),
        );

        expect(result).toEqual({
          testTable: {
            _data: [
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 6,
                object: { a: 1, b: 2 },
                string: 'world',
              },
            ],
          },
        });
      });

      it('with where searching null values', async () => {
        const result = rmhsh(
          await io.readRows({
            table: 'testTable',
            where: { null: null },
          }),
        );

        expect(result).toEqual({
          testTable: {
            _data: [
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 5,
                object: { a: 1, b: { c: 3 } },
                string: 'hello',
              },
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 6,
                object: { a: 1, b: 2 },
                string: 'world',
              },
            ],
          },
        });
      });

      it('with where searching boolean values', async () => {
        const result = rmhsh(
          await io.readRows({
            table: 'testTable',
            where: { boolean: true },
          }),
        );

        expect(result).toEqual({
          testTable: {
            _data: [
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 5,
                object: { a: 1, b: { c: 3 } },
                string: 'hello',
              },
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 6,
                object: { a: 1, b: 2 },
                string: 'world',
              },
            ],
          },
        });
      });

      it('with where searching array values', async () => {
        const result = rmhsh(
          await io.readRows({
            table: 'testTable',
            //where: { array: [1, 2, { a: 10 }] },
            where: {
              array: [1, 2, { a: 10, _hash: 'LeFJOCQVgToOfbUuKJQ-GO' }],
            },
          }),
        );

        expect(result).toEqual({
          testTable: {
            _data: [
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 5,
                object: { a: 1, b: { c: 3 } },
                string: 'hello',
              },
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 6,
                object: { a: 1, b: 2 },
                string: 'world',
              },
            ],
          },
        });
      });

      it('with where searching object values', async () => {
        const result = rmhsh(
          await io.readRows({
            table: 'testTable',
            //where: { object: { a: 1, b: { c: 3 } } },
            where: {
              object: {
                a: 1,
                b: { c: 3, _hash: 'yrqcsGrHfad4G4u9fgcAxY' },
                _hash: 'd-0fwNtdekpWJzLu4goUDI',
              },
            },
          }),
        );

        expect(result).toEqual({
          testTable: {
            _data: [
              {
                array: [1, 2, { a: 10 }],
                boolean: true,
                null: null,
                number: 5,
                object: { a: 1, b: { c: 3 } },
                string: 'hello',
              },
            ],
          },
        });
      });
    });

    it('should return an empty array if no rows match the where clause', async () => {
      await createExampleTable('testTable');

      await io.write({
        data: {
          testTable: {
            _type: 'ingredients',
            _data: [
              { a: 'value1', b: 'value2' },
              { a: 'value3', b: 'value4' },
            ],
          },
        },
      });

      const result = await io.readRows({
        table: 'testTable',
        where: { a: 'nonexistent' },
      });

      expect(result).toEqual({
        testTable: {
          _data: [],
        },
      });
    });

    it('should throw an error if the table does not exist', async () => {
      await expect(
        io.readRows({
          table: 'nonexistentTable',
          where: { column1: 'value1' },
        }),
      ).rejects.toThrow('Table nonexistentTable does not exist');
    });
  });

  describe('dump()', () => {
    it('returns a copy of the complete database', async () => {
      await expectGolden('io-mem/dump/empty.json').toBe(await io.dump());
      await createExampleTable('table1');
      await createExampleTable('table2');
      await expectGolden('io-mem/dump/two-tables.json').toBe(await io.dump());
    });
  });

  describe('dumpTable(request)', () => {
    it('returns a copy of the table', async () => {
      await createExampleTable('table1');

      await io.write({
        data: {
          table1: {
            _type: 'ingredients',
            _data: [{ a: 'a2' }],
          },
        },
      });

      await expectGolden('io-mem/dumpTable/table1.json').toBe(
        await io.dumpTable({ table: 'table1' }),
      );
    });

    it('throws an error if the table does not exist', async () => {
      await expect(io.dumpTable({ table: 'nonexistentTable' })).rejects.toThrow(
        'Failed to dump table nonexistentTable',
      );
    });
  });
});
