// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { JsonValue } from '@rljson/json';
import { iterateTables, Rljson, TableCfg } from '@rljson/rljson';

import Database from 'better-sqlite3';
import { mkdtemp } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

import { DsSqliteStandards } from './sqlite-standards.ts';

type DBType = Database.Database;
const _sql = new DsSqliteStandards();

/* v8 ignore start */

/**
 * Sqlite implementation of the Rljson Io interface.
 */
export class IoSqlite implements Io {
  private _db!: DBType;
  private _dbPath?: string;

  // ...........................................................................
  // Constructor & example
  constructor(public dbPath: string) {
    this._dbPath = dbPath;
    this._db = new Database(dbPath);
    this._init();
  }

  static example = async () => {
    const prefix = join(tmpdir(), 'io-sqlite-'); // prefix must end with '-'
    const newTempDir = await mkdtemp(prefix);
    console.log('New temp dir:', newTempDir);

    const tmpDb = join(newTempDir, 'example.sqlite');
    return new IoSqlite(tmpDb);
  };

  async deleteDatabase() {
    this._db.close();
    delete this._dbPath;
  }

  // ...........................................................................
  // General
  isReady() {
    return this._isReady.promise;
  }

  // ...........................................................................
  // Dump (export all data from database)

  dump(): Promise<Rljson> {
    return this._dump();
  }

  dumpTable(request: { table: string }): Promise<Rljson> {
    return this._dumpTable(request);
  }

  // ...........................................................................
  // Rows

  readRow(request: { table: string; rowHash: string }): Promise<Rljson> {
    return this._readRow(request);
  }

  readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    return this._readRows(request);
  }

  async tableCfgs(): Promise<Rljson> {
    const result: Rljson = {};
    const returnValue = this._db.prepare(_sql.currentTableCfgs).all();
    result.tableCfgs = JSON.parse(JSON.stringify(returnValue));
    return result;
  }

  async allTableNames(): Promise<string[]> {
    const query = _sql.allTableNames;
    const returnValue = this._db.prepare(query).all();
    const tableNames = (returnValue as { name: string }[]).map(
      (row) => row.name,
    );
    return tableNames;
  }

  // ...........................................................................
  // Write data into the respective table
  write(request: { data: Rljson }): Promise<void> {
    return this._write(request);
  }

  // ...........................................................................
  // Table management
  // createTable(request: { tableCfg: TableCfg }): Promise<void> {
  //   console.log(request.tableCfg);
  //   return Promise.resolve();
  // }

  createTable(request: { tableCfg: TableCfg }): Promise<void> {
    return this._createTable(request);
  }

  async tables(): Promise<Rljson> {
    const result: Rljson = {};
    return result;
  }

  // ######################
  // Private
  // ######################

  private _isReady = new IsReady();

  // ...........................................................................
  private async _init() {
    // Create tableCfgs table

    this._initTableCfgs();
    this._isReady.resolve();
  }

  // ...........................................................................
  private _initTableCfgs = () => {
    const tableCfg: TableCfg = {
      version: 1,
      key: 'tableCfgs',
      type: 'ingredients',
      columns: {
        version: { key: 'version', type: 'number', previous: 'number' },
        key: { key: 'key', type: 'string', previous: 'string' },
        type: { key: 'type', type: 'string', previous: 'string' },
        tableCfg: { key: 'tableCfg', type: 'string', previous: 'string' },
      },
    };

    hip(tableCfg);

    const tableCfgHashed = hsh(tableCfg);

    //create main table if it does not exist yet
    try {
      this._db.prepare(_sql.createMainTable).run();
    } catch (error) {
      console.error(error);
    }

    // Todo: Write tableCfg as first row into tableCfgs tables
    // As this is the first row to be entered, it is entered manually
    this._db
      .prepare(_sql.insertTableCfg)
      .run(
        tableCfgHashed._hash,
        tableCfg.version,
        tableCfg.key,
        tableCfg.type,
        JSON.stringify(tableCfg),
      );
  };

  // ...........................................................................
  private async _createTable(request: { tableCfg: TableCfg }): Promise<void> {
    // Create table in sqlite database
    // Check if given table name is valid
    _sql.isGood(request.tableCfg.key);
    _sql.isGood(request.tableCfg.type);

    //create config hash
    const tableCfgHashed = hsh(request.tableCfg);

    // Check if table exists
    const exists = this._db
      .prepare(_sql.tableCfg)
      .get(
        request.tableCfg.key,
        request.tableCfg.type,
        request.tableCfg.version,
      );

    if (!exists) {
      // Insert tableCfg into tableCfgs
      try {
        this._db
          .prepare(_sql.insertTableCfg)
          .run(
            tableCfgHashed._hash,
            request.tableCfg.version,
            request.tableCfg.key,
            request.tableCfg.type,
            JSON.stringify(request.tableCfg),
          );
      } catch (error) {
        console.error(error);
        throw error;
      }
    } else {
      throw new Error(`Table ${request.tableCfg.key} already exists`);
    }

    // Create actual table with name from tableCfg
    const tableName = request.tableCfg.key;
    const columnsCfg = request.tableCfg.columns;
    const columns = Object.keys(columnsCfg).flatMap((key) => {
      const column = columnsCfg[key];
      const sqliteType = _sql.dataType(column.type);
      if (sqliteType) {
        return `${column.key} ${sqliteType}`;
      }
      console.warn(
        `Skipping column ${column.key} with unsupported type ${column.type}`,
      );
      return [];
    });

    const columnsString = columns.join(', ');
    try {
      this._db.exec(_sql.createTable(tableName, columnsString));
    } catch (error) {
      console.error(`Error creating table ${tableName}:`, error);
    }

    // Get key and type from config
    // const { key, type } = config;
    const key = 'table1';

    // Check if, table already exists with key
    const existing = false; // Todo replace
    if (existing) {
      throw new Error(`Table ${key} already exists`);
    }

    // Create table

    console.log(request.tableCfg);

    // const createTableQuery =
    //   'CREATE TABLE ${   request.tableCfg.nam';
  }

  // ...........................................................................
  // private async _tables(): Promise<Rljson> {
  //   throw new Error('Not implemented');
  // }

  // ...........................................................................
  private async _readRow(request: {
    table: string;
    rowHash: string;
  }): Promise<Rljson> {
    throw new Error('Not implemented ' + request);
  }

  // ...........................................................................
  private async _readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    throw new Error('Not implemented ' + request);
  }

  // ...........................................................................

  private async _dump(): Promise<Rljson> {
    const returnFile: Rljson = {};
    const tables = this._db.prepare(_sql.allTableNames).all();

    for (const table of tables as { name: string }[]) {
      const tableDump: Rljson = await this._dumpTable({ table: table.name });
      const tableDumpJson = JSON.parse(JSON.stringify(tableDump));
      returnFile[table.name] = tableDumpJson;
    }

    return returnFile;
  }

  // ...........................................................................
  private async _dumpTable(request: { table: string }): Promise<Rljson> {
    let returnFile: Rljson = {};
    const returnValue = this._db.prepare(_sql.allData(request.table)).all();
    returnFile = JSON.parse(JSON.stringify(returnValue));
    return returnFile;
  }

  // ...........................................................................
  private async _write(request: { data: Rljson }): Promise<void> {
    // const tableName = Object.keys(request.data)[0];
    const hashedData = hsh(request.data);
    iterateTables(hashedData, (tableName, tableData) => {
      for (const row of tableData._data) {
        const columns = Object.keys(row);
        const placeholders = columns.map(() => '?').join(', ');
        const query = `INSERT INTO ${tableName} (${columns.join(
          ', ',
        )}) VALUES (${placeholders})`;
        const values = columns.map((column) => row[column]);
        this._db.prepare(query).run(...values);
      }
    });
  }

  public getTableColumns(tableName: string): { name: string; type: string }[] {
    const query = `PRAGMA table_info(${tableName})`;
    const result: { name: string; type: string }[] = [];
    // Assuming a database execution function `executeQuery` exists
    const rows = this._db.prepare(query).all() as {
      name: string;
      type: string;
    }[];
    for (const row of rows) {
      result.push({ name: row.name, type: row.type });
    }
    return result;
  }
}

/* v8 ignore stop */
