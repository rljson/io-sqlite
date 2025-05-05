// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
import { describe, expect, test } from 'vitest';

import { SqlStatements as SQL } from '../src/sql-statements';

// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

describe('SQlStatements', () => {
  test('tableName generates correct query', () => {
    const expectedQuery =
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?";
    expect(SQL.tableExists).toBe(expectedQuery);
  });

  test('foreignKeyList generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `PRAGMA foreign_key_list(${tableName})`;
    expect(SQL.foreignKeyList(tableName)).toBe(expectedQuery);
  });

  test('allColumns generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `PRAGMA table_info(${tableName})`;
    expect(SQL.columnKeys(tableName)).toBe(expectedQuery);
  });

  test('tableReferences generates correct query', () => {
    const referenceArray = ['col1', 'col2'];
    const suffixedReferences = referenceArray.map((col) => `${col}_col`);
    const expectedQuery = `FOREIGN KEY (col1_col) REFERENCES col1_ (_hash_col), FOREIGN KEY (col2_col) REFERENCES col2_ (_hash_col)`;
    expect(SQL.tableReferences(suffixedReferences)).toBe(expectedQuery);
  });

  test('allTableNames generates correct query', () => {
    const expectedQuery = `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
    expect(SQL.tableKeys).toBe(expectedQuery);
  });

  test('tableType returns correct type', () => {
    expect(SQL.tableType).toBe(
      'SELECT type_col AS type FROM tableCfgs_col WHERE key_col = ? ' +
        'AND version_col = (SELECT MAX(version_col) FROM tableCfgs_col WHERE key_col = ?)',
    );
  });

  test('suffix for tables returns correct suffix', () => {
    expect(SQL.suffix.tbl).toBe('_tbl');
  });

  test('suffix for columns returns correct suffix', () => {
    expect(SQL.suffix.col).toBe('_col');
  });

  test('suffix for temp tables returns correct suffix', () => {
    expect(SQL.suffix.tmp).toBe('_tmp');
  });

  test('tableReferences generates correct query', () => {
    const referenceArray = ['col1', 'col2'];
    const suffixedReferences = referenceArray.map((col) => `${col}_col`);
    const expectedQuery = `FOREIGN KEY (col1_col) REFERENCES col1_ (_hash_col), FOREIGN KEY (col2_col) REFERENCES col2_ (_hash_col)`;
    expect(SQL.tableReferences(suffixedReferences)).toBe(expectedQuery);
  });

  test('joinExpression generates correct query', () => {
    const alias = 't';
    const expectedQuery = `LEFT JOIN testTable AS t \n`;
    expect(SQL.joinExpression('testTable', alias)).toBe(expectedQuery);
  });

  test('articleExists generates correct query', () => {
    const expectedQuery =
      'SELECT cl.layer, ar.assign FROM catalogLayers cl\nLEFT JOIN articleSets ar\nON cl.articleSetsRef = ar._hash\nWHERE cl.winNumber = ? ';
    expect(SQL.articleExists).toBe(expectedQuery);
  });

  test('catalogExists generates correct query', () => {
    const expectedQuery = 'SELECT 1 FROM catalogLayers WHERE winNumber = ?';
    expect(SQL.catalogExists).toBe(expectedQuery);
  });

  test('catalogArticleTypes generates correct query', () => {
    const expectedQuery = `SELECT articleType FROM currentArticles\nWHERE winNumber = ?\nGROUP BY articleType`;
    expect(SQL.catalogArticleTypes).toBe(expectedQuery);
  });

  test('foreignKeyReferences generates correct query', () => {
    const columnNames = ['basicShapeWidthsRef', 'basicShapeDepthsRef'];
    const expectedQuery =
      'FOREIGN KEY (basicShapeWidthsRef_col) REFERENCES basicShapeWidths(' +
      '_hash_col), FOREIGN KEY (basicShapeDepthsRef_col) REFERENCES basicShapeDepths(' +
      '_hash_col)';

    expect(SQL.foreignKeyReferences(columnNames)).toBe(expectedQuery);
  });

  test('createMainTable generates correct query', () => {
    const expectedQuery = `CREATE TABLE tableCfgs_tbl (_hash_col TEXT, key_col TEXT, type_col TEXT, isHead_col INTEGER, isRoot_col INTEGER, isShared_col INTEGER, previous_col TEXT, columns_col TEXT)`;
    expect(SQL.createTableCfgsTable).toBe(expectedQuery);
  });

  test('createFullTable generates correct query', () => {
    const tableKey = 'testTable';
    const columnsDefinition = 'column1 TEXT, column2 TEXT';
    const foreignKeys = 'FOREIGN KEY (column1) REFERENCES table1(_hash)';
    const expectedQuery = `CREATE TABLE testTable (column1 TEXT, column2 TEXT, FOREIGN KEY (column1) REFERENCES table1(_hash))`;
    expect(SQL.createFullTable(tableKey, columnsDefinition, foreignKeys)).toBe(
      expectedQuery,
    );
  });

  test('dropTable generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `DROP TABLE IF EXISTS testTable${SQL.suffix.tbl}`;
    expect(SQL.dropTable(tableKey)).toBe(expectedQuery);
  });

  test('createTempTable generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `CREATE TABLE testTable${SQL.suffix.tmp} AS SELECT * FROM testTable${SQL.suffix.tbl}`;
    expect(SQL.createTempTable(tableKey)).toBe(expectedQuery);
  });

  test('dropTempTable generates correct query', () => {
    const tableKey = 'testTable';
    const expectedQuery = `DROP TABLE IF EXISTS testTable${SQL.suffix.tmp}`;
    expect(SQL.dropTempTable(tableKey)).toBe(expectedQuery);
  });

  test('fillTable generates correct query', () => {
    const tableKey = 'testTable';
    const commonColumns = 'column1, column2';
    const expectedQuery = `INSERT INTO testTable${SQL.suffix.tbl} (column1, column2) SELECT column1, column2 FROM testTable${SQL.suffix.tmp}`;
    expect(SQL.fillTable(tableKey, commonColumns)).toBe(expectedQuery);
  });

  test('deleteFromTable generates correct query', () => {
    const tableKey = 'testTable';
    const winNumber = '12345';
    const expectedQuery = `DELETE FROM testTable WHERE winNumber = '12345'`;
    expect(SQL.deleteFromTable(tableKey, winNumber)).toBe(expectedQuery);
  });

  test('addColumn generates correct query', () => {
    const tableKey = 'testTable';
    const columnName = 'newColumn';
    const columnType = 'TEXT';
    const expectedQuery = `ALTER TABLE testTable ADD COLUMN newColumn TEXT`;
    expect(SQL.addColumn(tableKey, columnName, columnType)).toBe(expectedQuery);
  });

  test('articleSetsRefs generates correct query', () => {
    const winNumber = '12345';
    const expectedQuery = `SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '12345'`;
    expect(SQL.articleSetsRefs(winNumber)).toBe(expectedQuery);
  });

  test('insertCurrentArticles generates correct query', () => {
    const expectedQuery = `INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)`;
    expect(SQL.insertCurrentArticles).toBe(expectedQuery);
  });

  test('currentCount generates correct query', () => {
    const tableKey = SQL.addTableSuffix('testTable');
    const expectedQuery = `SELECT COUNT(*) FROM ${tableKey}`;
    expect(SQL.currentCount(tableKey)).toBe(expectedQuery);
  });

  test('currentTableCfgs generates correct query', () => {
    const expectedQuery = [
      'SELECT *',
      'FROM tableCfgs_tbl',
      'WHERE _hash_col IN (',
      'WITH column_count AS (',
      'SELECT',
      '_hash_col,',
      'key_col,',
      'MAX(json_each.key) AS max_val',
      'FROM tableCfgs_tbl, json_each(columns_col)',
      'WHERE json_each.value IS NOT NULL',
      'GROUP BY _hash_col, key_col',
      '),',
      'max_tables AS (',
      'SELECT key_col, MAX(max_val) AS newest',
      'FROM column_count',
      'GROUP BY key_col',
      ')',
      'SELECT',
      'cc._hash_col',
      'FROM column_count cc',
      'LEFT JOIN max_tables mt',
      'ON cc.key_col = mt.key_col',
      'AND cc.max_val = mt.newest',
      'WHERE mt.newest IS NOT NULL',
      ');',
    ].join('\n');
    const statement = SQL.currentTableCfgs;
    expect(statement).toBe(expectedQuery);
  });
});
