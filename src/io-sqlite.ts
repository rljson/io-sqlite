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
    super(() => Promise.resolve(new Database(_dbPath)), sql);
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
    console.log('Creating example database at', tmpDb);
    return new IoSqlite(tmpDb, sql);
  };

  async deleteDatabase() {
    try {
      this.db.close();

      await rm(this._dbPath as string);
    } catch (e) {
      // Ignore error
      console.log('Error closing database:', e);
    }
  }

  public get currentPath(): PathLike {
    return this._dbPath as PathLike;
  }

  public get isOpen(): boolean {
    return super.isOpen && this.db.open;
  }

  public dbPath(): string {
    return this._dbPath;
  }
}
