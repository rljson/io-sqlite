// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { refName } from './constants.ts';


export abstract class SqlStandards {
  /// simple  keywords and statements*******************
  mainColumn: string = 'articleType';
  connectingColumn: string = '_hash';
  referenceIndicator: string = 'Ref';
  mainConnectingColumn: string = this.connectingColumn; //only to stay on the safe side
  tempSuffix: string = '_temp';
  queryIntro: string = 'SELECT DISTINCT';

  /// Postfix handling for the database
  public postFix: string = '_px';
  public mainTable: string = 'tableCfgs';

  // add postfix to a name in order to avoid name conflicts
  public addFix(name: string): string {
    return name === this.connectingColumn ? name : name + this.postFix;
  }
  // remove postfix from a name in order to communicate with the outside world
  public remFix(name: string): string {
    return name.endsWith(this.postFix)
      ? name.slice(0, -this.postFix.length)
      : name;
  }

  /// Parameterized queries*******************************

  public centralSource(winNumber: string) {
    return `(${this.queryIntro} winNumber, ${this.mainColumn} FROM ${this.mainTable} WHERE winNumber = '${winNumber}')`;
  }

  public joinExpression(tableName: string, alias: string) {
    return `LEFT JOIN ${tableName} AS ${alias} \n`;
  }

  public layerSource(winNumber: string, tableName: string) {
    return `(${this.queryIntro} ${this.mainColumn},\n ${this.mainConnectingColumn}
              FROM ${this.mainTable}\n WHERE winNumber = '${winNumber}'
              AND layer = '${tableName}')`;
  }

  public get articleExists() {
    return (
      'SELECT cl.layer, ar.assign FROM catalogLayers cl\n' +
      'LEFT JOIN articleSets ar\n' +
      'ON cl.articleSetsRef = ar._hash\n' +
      'WHERE cl.winNumber = ? '
    );
  }
  public get catalogExists() {
    return 'SELECT 1 FROM catalogLayers WHERE winNumber = ?';
  }
  public get catalogArticleTypes() {
    return (
      `SELECT articleType FROM currentArticles\n` +
      `WHERE winNumber = ?\n` +
      `GROUP BY articleType`
    );
  }

  public foreignKeyReferences(refColumnNames: string[]) {
    return refColumnNames
      .map(
        (col: string | any[]) =>
          `FOREIGN KEY (${col}) REFERENCES ${col.slice(0, -refName.length)}(${
            this.connectingColumn
          })`,
      )
      .join(', ')
      .toString();
  }

  /// Structure queries must be abstract as they are different for each database
  abstract tableName: string;
  abstract tableNames: string;

  abstract foreignKeyList(tableName: string): string;
  abstract columnNames(tableName: string): string;
  abstract tableReferences(referenceArray: string[]): string;
  abstract get tableExists(): string;

  /// Actions on tables

  public createBasicTable(tableName: string) {
    return `CREATE TABLE IF NOT EXISTS ${tableName} (${this.connectingColumn} TEXT PRIMARY KEY NOT NULL)`;
  }

  public createFullTable(
    tableName: string,
    columnsDefinition: string,
    foreignKeys: string,
  ) {
    return `CREATE TABLE ${tableName} (${columnsDefinition}, ${foreignKeys})`;
  }

  public dropTable(tableName: string) {
    return `DROP TABLE IF EXISTS ${tableName}`;
  }

  public createTempTable(tableName: string) {
    return `CREATE TABLE ${tableName}${this.tempSuffix} AS SELECT * FROM ${tableName}`;
  }

  public dropTempTable(tableName: string) {
    return `DROP TABLE IF EXISTS ${tableName}${this.tempSuffix}`;
  }

  public fillTable(tableName: string, commonColumns: string[]) {
    // select only those columns that are in both tables
    const columns = commonColumns.join(', ');
    return `INSERT INTO ${tableName} (${columns}) SELECT ${columns} FROM ${tableName}${this.tempSuffix}`;
  }

  public deleteFromTable(tableName: string, winNumber: string) {
    return `DELETE FROM ${tableName} WHERE winNumber = '${winNumber}'`;
  }

  public addColumn(tableName: string, columnName: string, columnType: string) {
    return `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`;
  }

  public selection(tableName: string, columns: string, whereClause: string) {
    return `SELECT ${columns} FROM ${tableName} WHERE ${whereClause}`;
  }

  public articleSetsRefs(winNumber: string) {
    return `SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '${winNumber}'`;
  }

  public get insertCurrentArticles() {
    return `INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)`;
  }

  public currentCount(tableName: string) {
    const fixedTableName = tableName.endsWith(this.postFix)
      ? tableName
      : this.addFix(tableName);
    return `SELECT COUNT(*) FROM ${fixedTableName}`;
  }

  public get createMainTable() {
    return `CREATE TABLE IF NOT EXISTS tableCfgs${this.postFix} (_hash TEXT PRIMARY KEY, version${this.postFix} INTEGER, key${this.postFix} TEXT KEY, type${this.postFix} TEXT, tableCfg${this.postFix} TEXT, previous${this.postFix} TEXT);`;
  }

  public get currentTableCfgs() {
    return `SELECT _hash, version${this.postFix} AS version, key${this.postFix} AS key, type${this.postFix} AS type, tableCfg${this.postFix} AS tableCfg, previous${this.postFix} AS previous
     FROM tableCfgs${this.postFix} t1
      WHERE version${this.postFix} = (
        SELECT MAX(version${this.postFix})
        FROM tableCfgs${this.postFix} t2
        WHERE t1.key${this.postFix} = t2.key${this.postFix} AND t1.type${this.postFix} = t2.type${this.postFix}
      )`;
  }

  public get currentTableCfg() {
    return `SELECT _hash, version${this.postFix} AS version, key${this.postFix} AS key, type${this.postFix} AS type, tableCfg${this.postFix} AS tableCfg, previous${this.postFix} AS previous
     FROM tableCfgs${this.postFix} t1
      WHERE version${this.postFix} = (
        SELECT MAX(version${this.postFix})
        FROM tableCfgs${this.postFix} t2
        WHERE t1.key${this.postFix} = t2.key${this.postFix} AND t1.type${this.postFix} = t2.type${this.postFix}
        AND t1.key${this.postFix} = ?
      )`;
  }

  public get tableTypeCheck() {
    return `SELECT type${this.postFix} FROM tableCfgs${this.postFix} WHERE key${this.postFix} = ?`;
  }
}
