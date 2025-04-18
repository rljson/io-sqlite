// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
import { expect, suite, test } from 'vitest';

import { SqlStandards } from '../src/sql-standards';

// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

class SqlStandardsImpl extends SqlStandards {
  public tableName = 'testTable';
  public tableNameWithPostfix = 'testTable' + SqlStandards.tablePostFix;
  tableNames = 'testTables';

  columnNames(tableName: string): string {
    console.log(tableName);
    throw new Error('Method not implemented.');
  }

  get tableExists(): string {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name=?`;
  }

  foreignKeyList(tableName: string): string {
    return `PRAGMA foreign_key_list(${tableName})`;
  }

  allColumns(tableName: string): string {
    return `PRAGMA table_info(${tableName})`;
  }

  tableReferences(referenceArray: string[]): string {
    return referenceArray
      .map(
        (col) =>
          `FOREIGN KEY (${col}) REFERENCES ${col.slice(
            0,
            -SqlStandards.referenceIndicator.length,
          )} (${SqlStandards.connectingColumn})`,
      )
      .join(', ');
  }
}

suite('SqlStandards', () => {
  const sqlStandards = new SqlStandardsImpl();

  test('centralSource generates correct query', () => {
    const winNumber = '12345';
    const expectedQuery = `(SELECT DISTINCT winNumber, articleType FROM tableCfgs WHERE winNumber = '12345')`;
    expect(sqlStandards.centralSource(winNumber)).toBe(expectedQuery);
  });

  test('joinExpression generates correct query', () => {
    const alias = 't';
    const expectedQuery = `LEFT JOIN testTable AS t \n`;
    expect(sqlStandards.joinExpression(sqlStandards.tableName, alias)).toBe(
      expectedQuery,
    );
  });

  test('layerSource generates correct query', () => {
    const winNumber = '12345';
    const tableName = 'testLayer';
    const expectedQuery = `(SELECT DISTINCT articleType,\n _hash
              FROM tableCfgs\n WHERE winNumber = '12345'
              AND layer = 'testLayer')`;
    expect(sqlStandards.layerSource(winNumber, tableName)).toBe(expectedQuery);
  });

  test('articleExists generates correct query', () => {
    const expectedQuery =
      'SELECT cl.layer, ar.assign FROM catalogLayers cl\nLEFT JOIN articleSets ar\nON cl.articleSetsRef = ar._hash\nWHERE cl.winNumber = ? ';
    expect(sqlStandards.articleExists).toBe(expectedQuery);
  });

  test('catalogExists generates correct query', () => {
    const expectedQuery = 'SELECT 1 FROM catalogLayers WHERE winNumber = ?';
    expect(sqlStandards.catalogExists).toBe(expectedQuery);
  });

  test('catalogArticleTypes generates correct query', () => {
    const expectedQuery = `SELECT articleType FROM currentArticles\nWHERE winNumber = ?\nGROUP BY articleType`;
    expect(sqlStandards.catalogArticleTypes).toBe(expectedQuery);
  });

  test('foreignKeyReferences generates correct query', () => {
    const columnNames = ['basicShapeWidthsRef', 'basicShapeDepthsRef'];
    const expectedQuery =
      'FOREIGN KEY (basicShapeWidthsRef) REFERENCES basicShapeWidths(_hash), FOREIGN KEY (basicShapeDepthsRef) REFERENCES basicShapeDepths(_hash)';
    expect(sqlStandards.foreignKeyReferences(columnNames)).toBe(expectedQuery);
  });

  test('createMainTable generates correct query', () => {
    const expectedQuery = `CREATE TABLE IF NOT EXISTS tableCfgs${SqlStandards.tablePostFix} (_hash TEXT PRIMARY KEY, version${SqlStandards.columnPostFix} INTEGER, key${SqlStandards.columnPostFix} TEXT KEY, type${SqlStandards.columnPostFix} TEXT, tableCfg${SqlStandards.columnPostFix} TEXT, previous${SqlStandards.columnPostFix} TEXT);`;
    expect(
      sqlStandards.createMainTable.replaceAll('\n', '').replaceAll(/\s+/g, ' '),
    ).toBe(expectedQuery);
  });

  test('createBasicTable generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `CREATE TABLE IF NOT EXISTS testTable (_hash TEXT PRIMARY KEY NOT NULL)`;
    expect(sqlStandards.createBasicTable(tableName)).toBe(expectedQuery);
  });

  test('createFullTable generates correct query', () => {
    const tableName = 'testTable';
    const columnsDefinition = 'column1 TEXT, column2 TEXT';
    const foreignKeys = 'FOREIGN KEY (column1) REFERENCES table1(_hash)';
    const expectedQuery = `CREATE TABLE testTable (column1 TEXT, column2 TEXT, FOREIGN KEY (column1) REFERENCES table1(_hash))`;
    expect(
      sqlStandards.createFullTable(tableName, columnsDefinition, foreignKeys),
    ).toBe(expectedQuery);
  });

  test('dropTable generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `DROP TABLE IF EXISTS testTable`;
    expect(sqlStandards.dropTable(tableName)).toBe(expectedQuery);
  });

  test('createTempTable generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `CREATE TABLE testTable_temp AS SELECT * FROM testTable`;
    expect(sqlStandards.createTempTable(tableName)).toBe(expectedQuery);
  });

  test('dropTempTable generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `DROP TABLE IF EXISTS testTable_temp`;
    expect(sqlStandards.dropTempTable(tableName)).toBe(expectedQuery);
  });

  test('fillTable generates correct query', () => {
    const tableName = 'testTable';
    const commonColumns = ['column1', 'column2'];
    const expectedQuery = `INSERT INTO testTable (column1, column2) SELECT column1, column2 FROM testTable_temp`;
    expect(sqlStandards.fillTable(tableName, commonColumns)).toBe(
      expectedQuery,
    );
  });

  test('deleteFromTable generates correct query', () => {
    const tableName = 'testTable';
    const winNumber = '12345';
    const expectedQuery = `DELETE FROM testTable WHERE winNumber = '12345'`;
    expect(sqlStandards.deleteFromTable(tableName, winNumber)).toBe(
      expectedQuery,
    );
  });

  test('addColumn generates correct query', () => {
    const tableName = 'testTable';
    const columnName = 'newColumn';
    const columnType = 'TEXT';
    const expectedQuery = `ALTER TABLE testTable ADD COLUMN newColumn TEXT`;
    expect(sqlStandards.addColumn(tableName, columnName, columnType)).toBe(
      expectedQuery,
    );
  });

  test('articleSetsRefs generates correct query', () => {
    const winNumber = '12345';
    const expectedQuery = `SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '12345'`;
    expect(sqlStandards.articleSetsRefs(winNumber)).toBe(expectedQuery);
  });

  test('insertCurrentArticles generates correct query', () => {
    const expectedQuery = `INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)`;
    expect(sqlStandards.insertCurrentArticles).toBe(expectedQuery);
  });

  test('currentCount generates correct query', () => {
    const expectedQuery = `SELECT COUNT(*) FROM ${sqlStandards.tableName}${SqlStandards.tablePostFix}`;
    expect(sqlStandards.currentCount(sqlStandards.tableName)).toBe(
      expectedQuery,
    );
  });
  test('currentCount generates correct query when table name is already fixed', () => {
    const expectedQuery = `SELECT COUNT(*) FROM ${sqlStandards.tableNameWithPostfix}`;
    expect(sqlStandards.currentCount(sqlStandards.tableNameWithPostfix)).toBe(
      expectedQuery,
    );
  });
});
