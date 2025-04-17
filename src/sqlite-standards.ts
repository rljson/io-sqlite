// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { SqlStandards } from './sql-standards.ts';

export class DsSqliteStandards extends SqlStandards {
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

  public get tableName() {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name=?`;
  }
  public get tableNames() {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
  }
  public foreignKeyList(tableName: string) {
    return `PRAGMA foreign_key_list(${tableName})`;
  }

  public columnNames(tableName: string) {
    return `PRAGMA table_info(${tableName})`;
  }

  // retrieve all data from a table using the original column names
  // i.e. without the postfix
  public allData(tableName: string, namedColumns?: string) {
    if (!namedColumns) {
      namedColumns = `*`;
    }
    return `SELECT ${namedColumns} FROM ${tableName}`;
  }

  public get tableCfg() {
    return `SELECT * FROM ${SqlStandards.mainTable}${SqlStandards.tablePostFix} WHERE key${SqlStandards.columnPostFix} = ? AND type${SqlStandards.columnPostFix} = ? AND version${SqlStandards.columnPostFix} = ?`;
  }

  public get insertTableCfg() {
    return `INSERT INTO ${SqlStandards.mainTable}${SqlStandards.tablePostFix} ( _hash, version${SqlStandards.columnPostFix}, key${SqlStandards.columnPostFix}, type${SqlStandards.columnPostFix}, tableCfg${SqlStandards.columnPostFix}) VALUES (?, ?, ?, ?, ?)`;
  }

  ///Equivalent data types
  sqliteDatatypes = [
    { type: 'string', sql: 'TEXT' },
    { type: 'jsonArray', sql: 'TEXT' },
    { type: 'json', sql: 'TEXT' },
    { type: 'number', sql: 'REAL' },
    { type: 'boolean', sql: 'INTEGER' },
    { type: 'undefined', sql: 'BLOB' },
    { type: 'null', sql: 'NULL' },
  ];

  public dataType(dataType: string) {
    const type = this.sqliteDatatypes.find((t) => t.type === dataType);
    if (!type) throw new Error(`Unknown data type: ${dataType}`);
    return type.sql;
  }

  public createTable(tableName: string, columns: string): string {
    if (!columns.includes('_hash')) {
      columns = `_hash TEXT PRIMARY KEY, ${columns}`;
    }

    return `CREATE TABLE IF NOT EXISTS ${tableName} (${columns})`;
  }
  public get tableExists() {
    return `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`;
  }

  public get tableType() {
    return `SELECT type${SqlStandards.columnPostFix} AS type FROM ${SqlStandards.mainTable}${SqlStandards.columnPostFix} WHERE key${SqlStandards.columnPostFix} = ? AND version${SqlStandards.columnPostFix} = (SELECT MAX(version${SqlStandards.columnPostFix}) FROM ${SqlStandards.mainTable}${SqlStandards.columnPostFix} WHERE key${SqlStandards.columnPostFix} = ?)`;
  }
}
