// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
import { ColumnCfg } from '@rljson/rljson';

// found in the LICENSE file in the root of this package.
import { beforeAll, describe, expect, test } from 'vitest';

import { SqlStatements } from '../src/sql-statements';

// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

describe('SQlStatements', () => {
  let sql: SqlStatements;

  beforeAll(() => {
    sql = new SqlStatements();
  });

  test('tableName generates correct query', () => {
    const expectedQuery =
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?";
    expect(sql.tableExists).toBe(expectedQuery);
  });

  test('foreignKeyList generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `PRAGMA foreign_key_list(${tableName})`;
    expect(sql.foreignKeyList(tableName)).toBe(expectedQuery);
  });

  test('allColumns generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `PRAGMA table_info(${tableName})`;
    expect(sql.columnKeys(tableName)).toBe(expectedQuery);
  });

  test('tableReferences generates correct query', () => {
    const referenceArray = ['oneTableRef', 'otherTableRef'];
    const expectedQuery =
      'FOREIGN KEY (oneTableRef_col) REFERENCES oneTable_tbl (_hash_col), FOREIGN KEY (otherTableRef_col) REFERENCES otherTable_tbl (_hash_col)';
    expect(sql.foreignKeys(referenceArray)).toBe(expectedQuery);
  });

  test('allTableNames generates correct query', () => {
    const expectedQuery = `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
    expect(sql.tableKeys).toBe(expectedQuery);
  });

  test('tableType returns correct type', () => {
    expect(sql.tableType).toBe(
      'SELECT type_col AS type FROM tableCfgs_col WHERE key_col = ? ' +
        'AND version_col = (SELECT MAX(version_col) FROM tableCfgs_col WHERE key_col = ?)',
    );
  });

  test('suffix for tables returns correct suffix', () => {
    expect(sql.suffix.tbl).toBe('_tbl');
  });

  test('suffix for columns returns correct suffix', () => {
    expect(sql.suffix.col).toBe('_col');
  });

  test('suffix for temp tables returns correct suffix', () => {
    expect(sql.suffix.tmp).toBe('_tmp');
  });

  test('joinExpression generates correct query', () => {
    const alias = 't';
    const expectedQuery = `LEFT JOIN testTable AS t \n`;
    expect(sql.joinExpression('testTable', alias)).toBe(expectedQuery);
  });

  test('articleExists generates correct query', () => {
    const expectedQuery =
      'SELECT cl.layer, ar.assign FROM catalogLayers cl\nLEFT JOIN articleSets ar\nON cl.articleSetsRef = ar._hash\nWHERE cl.winNumber = ? ';
    expect(sql.articleExists).toBe(expectedQuery);
  });

  test('catalogExists generates correct query', () => {
    const expectedQuery = 'SELECT 1 FROM catalogLayers WHERE winNumber = ?';
    expect(sql.catalogExists).toBe(expectedQuery);
  });

  test('catalogArticleTypes generates correct query', () => {
    const expectedQuery = `SELECT articleType FROM currentArticles\nWHERE winNumber = ?\nGROUP BY articleType`;
    expect(sql.catalogArticleTypes).toBe(expectedQuery);
  });

  test('foreignKeyReferences generates correct query', () => {
    const columnNames = ['basicShapeWidthsRef', 'basicShapeDepthsRef'];
    const expectedQuery =
      'FOREIGN KEY (basicShapeWidthsRef_col) REFERENCES basicShapeWidths(' +
      '_hash_col), FOREIGN KEY (basicShapeDepthsRef_col) REFERENCES basicShapeDepths(' +
      '_hash_col)';

    expect(sql.foreignKeyReferences(columnNames)).toBe(expectedQuery);
  });

  test('createMainTable generates correct query', () => {
    const expectedQuery = `CREATE TABLE tableCfgs_tbl (_hash_col TEXT PRIMARY KEY, key_col TEXT, type_col TEXT, isHead_col INTEGER, isRoot_col INTEGER, isShared_col INTEGER, previous_col TEXT, columns_col TEXT)`;
    expect(sql.createTableCfgsTable).toBe(expectedQuery);
  });

  test('createFullTable generates correct query', () => {
    const tableKey = 'testTable';
    const columnsDefinition = 'column1 TEXT, column2 TEXT';
    const foreignKeys = 'FOREIGN KEY (column1) REFERENCES table1(_hash)';
    const expectedQuery = `CREATE TABLE testTable (column1 TEXT, column2 TEXT, FOREIGN KEY (column1) REFERENCES table1(_hash))`;
    expect(sql.createFullTable(tableKey, columnsDefinition, foreignKeys)).toBe(
      expectedQuery,
    );
  });

  test('dropTable generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `DROP TABLE IF EXISTS testTable${sql.suffix.tbl}`;
    expect(sql.dropTable(tableKey)).toBe(expectedQuery);
  });

  test('createTempTable generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `CREATE TABLE testTable${sql.suffix.tmp} AS SELECT * FROM testTable${sql.suffix.tbl}`;
    expect(sql.createTempTable(tableKey)).toBe(expectedQuery);
  });

  test('dropTempTable generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `DROP TABLE IF EXISTS testTable${sql.suffix.tmp}`;
    expect(sql.dropTempTable(tableKey)).toBe(expectedQuery);
  });

  test('fillTable generates correct query', () => {
    const tableKey = 'testTable';
    const commonColumns = 'column1, column2';
    const expectedQuery = `INSERT INTO testTable${sql.suffix.tbl} (column1, column2) SELECT column1, column2 FROM testTable${sql.suffix.tmp}`;
    expect(sql.fillTable(tableKey, commonColumns)).toBe(expectedQuery);
  });

  test('deleteFromTable generates correct query', () => {
    const tableKey = 'testTable';
    const winNumber = '12345';
    const expectedQuery = `DELETE FROM testTable WHERE winNumber = '12345'`;
    expect(sql.deleteFromTable(tableKey, winNumber)).toBe(expectedQuery);
  });

  test('addColumn generates correct query', () => {
    const tableKey = 'testTable';
    const columnName = 'newColumn';
    const columnType = 'TEXT';
    const expectedQuery = `ALTER TABLE testTable ADD COLUMN newColumn TEXT`;
    expect(sql.addColumn(tableKey, columnName, columnType)).toBe(expectedQuery);
  });

  test('articleSetsRefs generates correct query', () => {
    const winNumber = '12345';
    const expectedQuery = `SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '12345'`;
    expect(sql.articleSetsRefs(winNumber)).toBe(expectedQuery);
  });

  test('insertCurrentArticles generates correct query', () => {
    const expectedQuery = `INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)`;
    expect(sql.insertCurrentArticles).toBe(expectedQuery);
  });

  test('currentCount generates correct query', () => {
    const tableKey = sql.addTableSuffix('testTable');
    const expectedQuery = `SELECT COUNT(*) FROM ${tableKey}`;
    expect(sql.currentCount(tableKey)).toBe(expectedQuery);
  });

  test('currentTableCfgs generates correct query', () => {
    const expectedQuery = [
      'SELECT',
      '  *',
      'FROM',
      '  tableCfgs_tbl',
      'WHERE',
      '  _hash_col IN (',
      '    WITH',
      '      column_count AS (',
      '        SELECT',
      '          _hash_col,',
      '          key_col,',
      '          MAX(json_each.key) AS max_val',
      '        FROM',
      '          tableCfgs_tbl,',
      '          json_each (columns_col)',
      '        WHERE',
      '          json_each.value IS NOT NULL',
      '        GROUP BY',
      '          _hash_col,',
      '          key_col',
      '      ),',
      '      max_tables AS (',
      '        SELECT',
      '          key_col,',
      '          MAX(max_val) AS newest',
      '        FROM',
      '          column_count',
      '        GROUP BY',
      '          key_col',
      '      )',
      '    SELECT',
      '      cc._hash_col',
      '    FROM',
      '      column_count cc',
      '      LEFT JOIN max_tables mt ON cc.key_col = mt.key_col',
      '      AND cc.max_val = mt.newest',
      '    WHERE',
      '      mt.newest IS NOT NULL',
      '  );',
    ].join('\n');
    const statement = sql.currentTableCfgs;
    expect(statement).toBe(expectedQuery);
  });

  test('tableCfgs generates correct query', () => {
    const expectedQuery = sql.tableCfgs;
    expect(expectedQuery).toBe(`SELECT * FROM tableCfgs_tbl`);
  });

  test('allData generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `SELECT * FROM testTable`;
    expect(sql.allData(tableKey)).toBe(expectedQuery);
  });

  test('tableKey', () => {
    const expectedQuery = `SELECT name FROM sqlite_master WHERE type='table' AND name=?`;
    expect(sql.tableKey).toBe(expectedQuery);
  });

  test('jsonToSqlType converts JSON value types to SQLite data types', () => {
    expect(sql.jsonToSqlType('string')).toBe('TEXT');
    expect(sql.jsonToSqlType('jsonArray')).toBe('TEXT');
    expect(sql.jsonToSqlType('json')).toBe('TEXT');
    expect(sql.jsonToSqlType('number')).toBe('REAL');
    expect(sql.jsonToSqlType('boolean')).toBe('INTEGER');
    expect(sql.jsonToSqlType('jsonValue')).toBe('TEXT');
  });

  test('removeColumnSuffix removes correct suffix', () => {
    const columnNameWithSuffix = 'testColumn_col';
    const columnNameWithoutSuffix = 'testColumn';
    expect(sql.removeColumnSuffix(columnNameWithSuffix)).toBe(
      columnNameWithoutSuffix,
    );
  });

  test('removeColumnSuffix does not modify names without suffix', () => {
    const columnName = 'testColumn';
    expect(sql.removeColumnSuffix(columnName)).toBe(columnName);
  });

  test('tableTypeCheck generates correct query', () => {
    const expectedQuery = `SELECT type_col FROM tableCfgs_tbl WHERE key_col = ?`;
    expect(sql.tableTypeCheck).toBe(expectedQuery);
  });

  test('rowCount generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `SELECT COUNT(*) FROM ${sql.addTableSuffix(
      tableKey,
    )}`;
    expect(sql.rowCount(tableKey)).toBe(expectedQuery);
  });

  test('alterTable generates correct queries for added columns', () => {
    const tableKey = 'testTable';
    const addedColumns: ColumnCfg[] = [
      { key: 'newColumn1', type: 'string' },
      { key: 'newColumn2', type: 'number' },
    ];
    const expectedQueries = [
      `ALTER TABLE testTable_tbl ADD COLUMN newColumn1_col TEXT;`,
      `ALTER TABLE testTable_tbl ADD COLUMN newColumn2_col REAL;`,
    ];
    expect(sql.alterTable(tableKey, addedColumns)).toEqual(expectedQueries);
  });
});
