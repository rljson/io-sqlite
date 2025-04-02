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
            -this.referenceIndicator.length,
          )} (${this.connectingColumn})`,
      )
      .join(', ');
  }
  public get allTableNames() {
    return `SELECT name FROM sqlite_master WHERE type='table' and name NOT LIKE 'sqlite_%'`;
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

  public allColumnNames(tableName: string) {
    return `PRAGMA table_info(${tableName})`;
  }

  public allData(tableName: string) {
    return `SELECT * FROM ${tableName}`;
  }

  public get tableCfg() {
    return `SELECT * FROM tableCfgs WHERE key = ? AND type = ? AND version = ?`;
  }

  public get insertTableCfg() {
    return `INSERT INTO tableCfgs ( _hash, version, key, type, tableCfg) VALUES (?, ?, ?, ?, ?)`;
  }

  ///Equivalent data types
  sqliteDatatypes = [
    { type: 'string', sql: 'TEXT' },
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

  public isGood(term: string): string {
    if (this._reservedKeywords().includes(term.toUpperCase())) {
      throw new Error(
        `The term "${term}" is a reserved keyword and cannot be used.`,
      );
    }
    return term;
  }

  private _reservedKeywords(): string[] {
    return [
      'ABORT',
      'ACTION',
      'ADD',
      'AFTER',
      'ALL',
      'ALTER',
      'ANALYZE',
      'AND',
      'AS',
      'ASC',
      'ATTACH',
      'AUTOINCREMENT',
      'BEFORE',
      'BEGIN',
      'BETWEEN',
      'BY',
      'CASCADE',
      'CASE',
      'CAST',
      'CHECK',
      'COLLATE',
      'COLUMN',
      'COMMIT',
      'CONFLICT',
      'CONSTRAINT',
      'CREATE',
      'CROSS',
      'CURRENT_DATE',
      'CURRENT_TIME',
      'CURRENT_TIMESTAMP',
      'DATABASE',
      'DEFAULT',
      'DEFERRABLE',
      'DEFERRED',
      'DELETE',
      'DESC',
      'DETACH',
      'DISTINCT',
      'DROP',
      'EACH',
      'ELSE',
      'END',
      'ESCAPE',
      'EXCEPT',
      'EXCLUSIVE',
      'EXISTS',
      'EXPLAIN',
      'FAIL',
      'FOR',
      'FOREIGN',
      'FROM',
      'FULL',
      'GLOB',
      'GROUP',
      'HAVING',
      'IF',
      'IGNORE',
      'IMMEDIATE',
      'IN',
      'INDEX',
      'INDEXED',
      'INITIALLY',
      'INNER',
      'INSERT',
      'INSTEAD',
      'INTERSECT',
      'INTO',
      'IS',
      'ISNULL',
      'JOIN',
      'KEY',
      'LEFT',
      'LIKE',
      'LIMIT',
      'MATCH',
      'NATURAL',
      'NO',
      'NOT',
      'NOTNULL',
      'NULL',
      'OF',
      'OFFSET',
      'ON',
      'OR',
      'ORDER',
      'OUTER',
      'PLAN',
      'PRAGMA',
      'PRIMARY',
      'QUERY',
      'RAISE',
      'RECURSIVE',
      'REFERENCES',
      'REGEXP',
      'REINDEX',
      'RELEASE',
      'RENAME',
      'REPLACE',
      'RESTRICT',
      'RIGHT',
      'ROLLBACK',
      'ROW',
      'SAVEPOINT',
      'SELECT',
      'SET',
      'TABLE',
      'TEMP',
      'TEMPORARY',
      'THEN',
      'TO',
      'TRANSACTION',
      'TRIGGER',
      'UNION',
      'UNIQUE',
      'UPDATE',
      'USING',
      'VACUUM',
      'VALUES',
      'VIEW',
      'VIRTUAL',
      'WHEN',
      'WHERE',
      'WITH',
      'WITHOUT',
    ];
  }
}
