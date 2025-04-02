// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { refName } from './constants.ts';


export abstract class SqlStandards {
  /// simple  keywords and statements*******************
  mainTable: string = 'current_articles';
  mainColumn: string = 'articleType';
  connectingColumn: string = '_hash';
  referenceIndicator: string = 'Ref';
  mainConnectingColumn: string = this.connectingColumn; //only to stay on the safe side
  tempSuffix: string = '_temp';
  queryIntro: string = 'SELECT DISTINCT';

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
  abstract allColumnNames(tableName: string): string;
  abstract get allTableNames(): string;
  abstract tableReferences(referenceArray: string[]): string;

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

  public refillTable(tableName: string, commonColumns: string[]) {
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

  public articleSetsRefs(winNumber: string) {
    return `SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '${winNumber}'`;
  }

  public get insertCurrentArticles() {
    return `INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)`;
  }

  public currentCount(tableName: string) {
    return `SELECT COUNT(*) FROM ${tableName}`;
  }

  public get createMainTable() {
    return 'CREATE TABLE IF NOT EXISTS tableCfgs (_hash TEXT PRIMARY KEY, version INTEGER, key TEXT KEY, type TEXT, tableCfg TEXT, previous TEXT);';
  }

  public get currentTableCfgs() {
    return `SELECT * FROM tableCfgs t1
      WHERE version = (
        SELECT MAX(version)
        FROM tableCfgs t2
        WHERE t1.key = t2.key AND t1.type = t2.type
      )`;
  }
}
