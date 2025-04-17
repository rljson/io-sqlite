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
  static postFix: string = '_px';
  static mainTable: string = 'tableCfgs';
  static revisionsTable: string = 'revisions';

  // add postfix to a name in order to avoid name conflicts
  static addFix(name: string): string {
    return name === SqlStandards.connectingColumn
      ? name
      : name + SqlStandards.postFix;
  }
  // remove postfix from a name in order to communicate with the outside world
  static remFix(name: string): string {
    return name.endsWith(SqlStandards.postFix)
      ? name.slice(0, -SqlStandards.postFix.length)
      : name;
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
    const fixedTableName = tableName.endsWith(SqlStandards.postFix)
      ? tableName
      : SqlStandards.addFix(tableName);
    return `SELECT COUNT(*) FROM ${fixedTableName}`;
  }

  public get createMainTable() {
    return `CREATE TABLE IF NOT EXISTS tableCfgs${SqlStandards.postFix} (_hash TEXT PRIMARY KEY, version${SqlStandards.postFix} INTEGER, key${SqlStandards.postFix} TEXT KEY, type${SqlStandards.postFix} TEXT, tableCfg${SqlStandards.postFix} TEXT, previous${SqlStandards.postFix} TEXT);`;
  }

  public get currentTableCfgs() {
    return `SELECT _hash, version${SqlStandards.postFix} AS version, key${SqlStandards.postFix} AS key, type${SqlStandards.postFix} AS type, tableCfg${SqlStandards.postFix} AS tableCfg, previous${SqlStandards.postFix} AS previous
     FROM tableCfgs${SqlStandards.postFix} t1
      WHERE version${SqlStandards.postFix} = (
        SELECT MAX(version${SqlStandards.postFix})
        FROM tableCfgs${SqlStandards.postFix} t2
        WHERE t1.key${SqlStandards.postFix} = t2.key${SqlStandards.postFix} AND t1.type${SqlStandards.postFix} = t2.type${SqlStandards.postFix}
      )`;
  }

  public get currentTableCfg() {
    return `SELECT _hash, version${SqlStandards.postFix} AS version, key${SqlStandards.postFix} AS key, type${SqlStandards.postFix} AS type, tableCfg${SqlStandards.postFix} AS tableCfg, previous${SqlStandards.postFix} AS previous
     FROM tableCfgs${SqlStandards.postFix} t1
      WHERE version${SqlStandards.postFix} = (
        SELECT MAX(version${SqlStandards.postFix})
        FROM tableCfgs${SqlStandards.postFix} t2
        WHERE t1.key${SqlStandards.postFix} = t2.key${SqlStandards.postFix} AND t1.type${SqlStandards.postFix} = t2.type${SqlStandards.postFix}
        AND t1.key${SqlStandards.postFix} = ?
      )`;
  }

  public get tableTypeCheck() {
    return `SELECT type${SqlStandards.postFix} FROM tableCfgs${SqlStandards.postFix} WHERE key${SqlStandards.postFix} = ?`;
  }
}
