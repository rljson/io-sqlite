// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

//****************************************************** */
//These sql statements are partly based on the SQLite database;
//see https://www.sqlite.org/lang.html
// and https://www.sqlite.org/cli.html
//****************************************************** */

import { IoTools } from '@rljson/io';
import { JsonValueType } from '@rljson/json';
import { ColumnCfg, TableCfg, TableKey } from '@rljson/rljson';

import { refName } from './constants.ts';


export class SqlStatements {
  /// simple  keywords and statements*******************
  static connectingColumn: string = '_hash';
  static queryIntro: string = 'SELECT DISTINCT';

  // Names for the main tables in the database
  static tbl: { [key: string]: string } = {
    main: 'tableCfgs',
    revision: 'revisions',
  };

  /// Postfix handling for the database
  static suffix: { [key: string]: string } = {
    col: '_col',
    tbl: '_tbl',
    tmp: '_tmp',
    ref: 'Ref',
  };

  /**
   * Converts a JSON value type to an SQLite data type.
   * @param dataType - The JSON value type to convert.
   * @returns - The corresponding SQLite data type.
   */
  static jsonToSqlType(dataType: JsonValueType): string {
    switch (dataType) {
      case 'string':
        return 'TEXT';
      case 'jsonArray':
        return 'TEXT';
      case 'json':
        return 'TEXT';
      case 'number':
        return 'REAL';
      case 'boolean':
        return 'INTEGER';
      case 'jsonValue':
        return 'TEXT';
    }
  }

  static get tableKey() {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name=?`;
  }
  static get tableKeys() {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
  }

  static rowCount(tableKey: string) {
    return `SELECT COUNT(*) FROM ${this.addTableSuffix(tableKey)}`;
  }
  static foreignKeyList(tableKey: string) {
    return `PRAGMA foreign_key_list(${tableKey})`;
  }

  static allData(tableKey: string, namedColumns?: string) {
    if (!namedColumns) {
      namedColumns = `*`;
    }
    return `SELECT ${namedColumns} FROM ${tableKey}`;
  }

  static get tableCfg() {
    return `SELECT * FROM ${this.tbl.main}${this.suffix.tbl} WHERE key${this.suffix.col} = ?`;
  }

  static get tableCfgs() {
    return `SELECT * FROM ${this.tbl.main}${this.suffix.tbl}`;
  }
  static addColumnSuffix(name: string): string {
    return this.addFix(name, this.suffix.col);
  }

  static addTableSuffix(name: string): string {
    return this.addFix(name, this.suffix.tbl);
  }

  // add suffix to a name in order to avoid name conflicts
  static addFix(name: string, fix: string): string {
    return name.endsWith(fix) ? name : name + fix;
  }

  static removeTableSuffix(name: string): string {
    return this.remFix(name, this.suffix.tbl);
  }

  static removeColumnSuffix(name: string): string {
    return this.remFix(name, this.suffix.col);
  }

  // remove suffix from a name in order to communicate with the outside world
  static remFix(name: string, fix: string): string {
    return name.endsWith(fix) ? name.slice(0, -fix.length) : name;
  }

  static joinExpression(tableKey: string, alias: string) {
    return `LEFT JOIN ${tableKey} AS ${alias} \n`;
  }

  static get articleExists() {
    return (
      'SELECT cl.layer, ar.assign FROM catalogLayers cl\n' +
      'LEFT JOIN articleSets ar\n' +
      'ON cl.articleSetsRef = ar._hash\n' +
      'WHERE cl.winNumber = ? '
    );
  }
  static get catalogExists() {
    return 'SELECT 1 FROM catalogLayers WHERE winNumber = ?';
  }
  static get catalogArticleTypes() {
    return (
      `SELECT articleType FROM currentArticles\n` +
      `WHERE winNumber = ?\n` +
      `GROUP BY articleType`
    );
  }

  static foreignKeyReferences(refColumnNames: string[]) {
    return refColumnNames
      .map(
        (col: string | any[]) =>
          `FOREIGN KEY (${col}${this.suffix.col}) REFERENCES ${col.slice(
            0,
            -refName.length,
          )}(${this.addColumnSuffix(this.connectingColumn)})`,
      )
      .join(', ');
  }

  static tableReferences(referenceArray: string[]): string {
    return referenceArray
      .map(
        (col) =>
          `FOREIGN KEY (${col}) REFERENCES ${col.slice(
            0,
            -this.suffix.ref.length,
          )} (${this.addColumnSuffix(this.connectingColumn)})`,
      )
      .join(', ');
  }

  static insertTableCfg() {
    const columnKeys = IoTools.tableCfgsTableCfg.columns.map((col) => col.key);
    const columnKeysWithPostfix = columnKeys.map((col) =>
      this.addColumnSuffix(col),
    );
    const columnsSql = columnKeysWithPostfix.join(', ');
    const valuesSql = '?, '.repeat(columnKeys.length - 1) + '?';

    return `INSERT INTO ${this.tbl.main}${this.suffix.tbl} ( ${columnsSql} ) VALUES (${valuesSql})`;
  }

  static get tableExists() {
    return `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`;
  }

  static get tableType() {
    return `SELECT type${this.suffix.col} AS type FROM ${this.tbl.main}${this.suffix.col} WHERE key${this.suffix.col} = ? AND version${this.suffix.col} = (SELECT MAX(version${this.suffix.col}) FROM ${this.tbl.main}${this.suffix.col} WHERE key${this.suffix.col} = ?)`;
  }

  static columnKeys(tableKey: string) {
    return `PRAGMA table_info(${tableKey})`;
  }

  static createFullTable(
    tableKey: string,
    columnsDefinition: string,
    foreignKeys: string,
  ) {
    return `CREATE TABLE ${tableKey} (${columnsDefinition}, ${foreignKeys})`;
  }

  static dropTable(tableKey: string) {
    return `DROP TABLE IF EXISTS ${tableKey}${this.suffix.tbl}`;
  }

  static createTempTable(tableKey: string) {
    return `CREATE TABLE ${tableKey}${this.suffix.tmp} AS SELECT * FROM ${tableKey}${this.suffix.tbl}`;
  }

  static dropTempTable(tableKey: string) {
    return `DROP TABLE IF EXISTS ${tableKey}${this.suffix.tmp}`;
  }

  static fillTable(tableKey: string, commonColumns: string) {
    // select only those columns that are in both tables
    return `INSERT INTO ${tableKey}${this.suffix.tbl} (${commonColumns}) SELECT ${commonColumns} FROM ${tableKey}${this.suffix.tmp}`;
  }

  static deleteFromTable(tableKey: string, winNumber: string) {
    return `DELETE FROM ${tableKey} WHERE winNumber = '${winNumber}'`;
  }

  static addColumn(tableKey: string, columnName: string, columnType: string) {
    return `ALTER TABLE ${tableKey} ADD COLUMN ${columnName} ${columnType}`;
  }

  static selection(tableKey: string, columns: string, whereClause: string) {
    return `SELECT ${columns} FROM ${tableKey} WHERE ${whereClause}`;
  }

  static articleSetsRefs(winNumber: string) {
    return `SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '${winNumber}'`;
  }

  static get insertCurrentArticles() {
    return `INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)`;
  }

  static currentCount(tableKey: string) {
    return `SELECT COUNT(*) FROM ${this.addTableSuffix(tableKey)}`;
  }

  static createTable(tableCfg: TableCfg): string {
    const sqltableKey = this.addTableSuffix(tableCfg.key);
    const columnsCfg = tableCfg.columns;

    const sqlCreateColumns = columnsCfg
      .map((col) => {
        const sqliteType = this.jsonToSqlType(col.type);
        return `${this.addColumnSuffix(col.key)} ${sqliteType}`;
      })
      .join(', ');

    // standard primary key - do not remove ;-)
    const colsWithPrimaryKey = sqlCreateColumns.replace(
      /_hash TEXT/g,
      '_hash TEXT PRIMARY KEY',
    );

    // foreign keys if there are any (not yet implemented again)
    const foreignKeys = this.tableReferences(
      columnsCfg
        .map((col) => col.key)
        .filter((col) => col.endsWith(this.suffix.ref)),
    );
    console.log('foreignKeys', foreignKeys);
    const sqlForeignKeys = foreignKeys ? `, ${foreignKeys}` : '';
    return `CREATE TABLE ${sqltableKey} (${colsWithPrimaryKey}${sqlForeignKeys})`;
    //return `CREATE TABLE ${sqltableKey} (${sqlCreateColumns})`;
  }

  static alterTable(tableKey: TableKey, addedColumns: ColumnCfg[]): string[] {
    const tableKeyWithSuffix = this.addTableSuffix(tableKey);
    const statements: string[] = [];
    for (const col of addedColumns) {
      const columnKey = this.addColumnSuffix(col.key);
      const columnType = this.jsonToSqlType(col.type);
      statements.push(
        `ALTER TABLE ${tableKeyWithSuffix} ADD COLUMN ${columnKey} ${columnType};`,
      );
    }

    return statements;
  }

  static get createTableCfgsTable() {
    return this.createTable(IoTools.tableCfgsTableCfg);
  }

  static get currentTableCfgs() {
    // TODO: Muss dynamisch generiert werden aus IoTools.tableCfgsTableCfg

    const result = [
      `SELECT _hash, version${this.suffix.col}, key${this.suffix.col}, type${this.suffix.col}, columns${this.suffix.col}, previous${this.suffix.col}`,
      `FROM tableCfgs${this.suffix.tbl} t1`,
      `WHERE version${this.suffix.col} = (`,
      `  SELECT MAX(version${this.suffix.col})`,
      `  FROM tableCfgs${this.suffix.tbl} t2`,
      `  WHERE t1.key${this.suffix.col} = t2.key${this.suffix.col} AND t1.type${this.suffix.col} = t2.type${this.suffix.col}`,
      `)`,
    ];

    return result.join('\n');
  }

  static get currentTableCfg() {
    return `SELECT _hash, version${this.suffix.col}, key${this.suffix.col}, type${this.suffix.col}, columns${this.suffix.col}, previous${this.suffix.col}
     FROM tableCfgs${this.suffix.tbl} t1
      WHERE version${this.suffix.col} = (
        SELECT MAX(version${this.suffix.col})
        FROM tableCfgs${this.suffix.tbl} t2
        WHERE t1.key${this.suffix.col} = t2.key${this.suffix.col} AND t1.type${this.suffix.col} = t2.type${this.suffix.col}
        AND t1.key${this.suffix.col} = ?
      )`;
  }

  static get tableTypeCheck() {
    return `SELECT type${this.suffix.col} FROM tableCfgs${this.suffix.tbl} WHERE key${this.suffix.col} = ?`;
  }
}
