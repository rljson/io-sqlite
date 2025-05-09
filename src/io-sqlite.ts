// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import Database from 'better-sqlite3';
import { PathLike } from 'fs';
import { rm } from 'fs/promises';

import { IoSql } from './io-sql.ts';
import { SqlStatements } from './sql-statements.ts';

export class IoSqlite extends IoSql {
  constructor(private readonly _dbPath: string, sql: SqlStatements) {
    const db = new Database(_dbPath);
    super(db, sql);
  }

  /**
   * Returns an example database
   * @param dbDir - The directory to store the database file.
   * If not provided, a temporary directory will be created.
   * @param sql - The SQL statements to use.
   * If not provided, the default SqlStatements will be used.
   */
  static example = async (
    dbDir: string | undefined = undefined,
    sql: SqlStatements | undefined = new SqlStatements(),
  ) => {
    const tmpDb = await this.exampleDbFilePath(dbDir);
    return new IoSqlite(tmpDb, sql);
  };

  async deleteDatabase() {
    this.close();
    await rm(this._dbPath as string);
  }

  public get currentPath(): PathLike {
    return this._dbPath as PathLike;
  }
}
