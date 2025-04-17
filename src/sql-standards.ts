// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { refName } from './constants.ts';

export abstract class SqlStandards {
  /// simple  keywords and statements*******************
  static mainColumn: string = 'articleType';
  static connectingColumn: string = '_hash';
  static referenceIndicator: string = 'Ref';
  static mainConnectingColumn: string = SqlStandards.connectingColumn; //only to stay on the safe side
  static tempSuffix: string = '_temp';
  static queryIntro: string = 'SELECT DISTINCT';

  /// Postfix handling for the database
  static columnPostFix: string = '_col';
  static tablePostFix: string = '_tbl';
  static mainTable: string = 'tableCfgs';
  static revisionsTable: string = 'revisions';

  static addColumnPostFix(name: string): string {
    return this.addFix(name, SqlStandards.columnPostFix);
  }

  static addTablePostFix(name: string): string {
    return this.addFix(name, SqlStandards.tablePostFix);
  }

  // add postfix to a name in order to avoid name conflicts
  static addFix(name: string, fix: string): string {
    return name === SqlStandards.connectingColumn ? name : name + fix;
  }

  static removeTablePostFix(name: string): string {
    return this.remFix(name, SqlStandards.tablePostFix);
  }

  static removeColumnPostFix(name: string): string {
    return this.remFix(name, SqlStandards.columnPostFix);
  }

  // remove postfix from a name in order to communicate with the outside world
  static remFix(name: string, fix: string): string {
    return name.endsWith(fix) ? name.slice(0, -fix.length) : name;
  }

  /// Parameterized queries*******************************

  public centralSource(winNumber: string) {
    return `(${SqlStandards.queryIntro} winNumber, ${SqlStandards.mainColumn} FROM ${SqlStandards.mainTable} WHERE winNumber = '${winNumber}')`;
  }

  public joinExpression(tableName: string, alias: string) {
    return `LEFT JOIN ${tableName} AS ${alias} \n`;
  }

  public layerSource(winNumber: string, tableName: string) {
    return `(${SqlStandards.queryIntro} ${SqlStandards.mainColumn},\n ${SqlStandards.mainConnectingColumn}
              FROM ${SqlStandards.mainTable}\n WHERE winNumber = '${winNumber}'
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
            SqlStandards.connectingColumn
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
    return `CREATE TABLE IF NOT EXISTS ${tableName} (${SqlStandards.connectingColumn} TEXT PRIMARY KEY NOT NULL)`;
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
    return `CREATE TABLE ${tableName}${SqlStandards.tempSuffix} AS SELECT * FROM ${tableName}`;
  }

  public dropTempTable(tableName: string) {
    return `DROP TABLE IF EXISTS ${tableName}${SqlStandards.tempSuffix}`;
  }

  public fillTable(tableName: string, commonColumns: string[]) {
    // select only those columns that are in both tables
    const columns = commonColumns.join(', ');
    return `INSERT INTO ${tableName} (${columns}) SELECT ${columns} FROM ${tableName}${SqlStandards.tempSuffix}`;
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
    const fixedTableName = tableName.endsWith(SqlStandards.tablePostFix)
      ? tableName
      : SqlStandards.addTablePostFix(tableName);
    return `SELECT COUNT(*) FROM ${fixedTableName}`;
  }

  public get createMainTable() {
    return `CREATE TABLE IF NOT EXISTS tableCfgs${SqlStandards.tablePostFix}
      (_hash TEXT PRIMARY KEY, version${SqlStandards.columnPostFix} INTEGER, key${SqlStandards.columnPostFix} TEXT KEY, type${SqlStandards.columnPostFix} TEXT, tableCfg${SqlStandards.columnPostFix} TEXT, previous${SqlStandards.columnPostFix} TEXT);`;
  }

  public get currentTableCfgs() {
    return `SELECT _hash, version${SqlStandards.columnPostFix} AS version, key${SqlStandards.columnPostFix} AS key, type${SqlStandards.columnPostFix} AS type, tableCfg${SqlStandards.columnPostFix} AS tableCfg, previous${SqlStandards.columnPostFix} AS previous
     FROM tableCfgs${SqlStandards.tablePostFix} t1
      WHERE version${SqlStandards.columnPostFix} = (
        SELECT MAX(version${SqlStandards.columnPostFix})
        FROM tableCfgs${SqlStandards.tablePostFix} t2
        WHERE t1.key${SqlStandards.columnPostFix} = t2.key${SqlStandards.columnPostFix} AND t1.type${SqlStandards.columnPostFix} = t2.type${SqlStandards.columnPostFix}
      )`;
  }

  public get currentTableCfg() {
    return `SELECT _hash, version${SqlStandards.columnPostFix} AS version, key${SqlStandards.columnPostFix} AS key, type${SqlStandards.columnPostFix} AS type, tableCfg${SqlStandards.columnPostFix} AS tableCfg, previous${SqlStandards.columnPostFix} AS previous
     FROM tableCfgs${SqlStandards.tablePostFix} t1
      WHERE version${SqlStandards.columnPostFix} = (
        SELECT MAX(version${SqlStandards.columnPostFix})
        FROM tableCfgs${SqlStandards.tablePostFix} t2
        WHERE t1.key${SqlStandards.columnPostFix} = t2.key${SqlStandards.columnPostFix} AND t1.type${SqlStandards.columnPostFix} = t2.type${SqlStandards.columnPostFix}
        AND t1.key${SqlStandards.columnPostFix} = ?
      )`;
  }

  public get tableTypeCheck() {
    return `SELECT type${SqlStandards.columnPostFix} FROM tableCfgs${SqlStandards.tablePostFix} WHERE key${SqlStandards.columnPostFix} = ?`;
  }
}
