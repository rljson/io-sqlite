// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { JsonValue } from '@rljson/json';
import { Rljson, TableCfg, TableCfgRef } from '@rljson/rljson';

import Database from 'better-sqlite3';
import { mkdtemp } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

type DBType = Database.Database;

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

  // ...........................................................................
  // Write

  write(request: { data: Rljson }): Promise<void> {
    return this._write(request);
  }

  // ...........................................................................
  // Table management
  createTable(request: { tableCfg: string }): Promise<void> {
    return this._createTable(request);
  }

  async tables(): Promise<Rljson> {
    return this._tables();
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
        key: { key: 'key', type: 'string', previous: 'string' },
        type: { key: 'type', type: 'string', previous: 'string' },
      },
    };

    hip(tableCfg);

    const tableCfgHashed = hsh(tableCfg);

    //create main table if it does not exist yet
    try {
      this._db
        .prepare(
          `
      CREATE TABLE IF NOT EXISTS tableCfgs (
        _hash TEXT PRIMARY KEY,
        version INTEGER,
        key TEXT KEY,
        type TEXT,
        previous TEXT
      );
    `,
        )
        .run();
    } catch (error) {
      console.error(error);
    }

    // Todo: Write tableCfg as first row into tableCfgs tables
    // As this is the first row to be entered, it is entered manually
    this._db
      .prepare(
        `
      INSERT INTO tableCfgs (_hash, version, key, type) VALUES (?, ?, ?, ?);
    `,
      )
      .run(tableCfgHashed._hash, tableCfg.version, tableCfg.key, tableCfg.type);
  };

  // ...........................................................................
  private async _createTable(request: {
    tableCfg: TableCfgRef;
  }): Promise<void> {
    // Get table cfg with hash "tableCfg" from table tableCfgs
    const config = {}; // Todo replace

    if (!config) {
      throw new Error(`Table config ${request.tableCfg} not found`);
    }

    // Get key and type from config
    // const { key, type } = config;
    const key = 'xyz';

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
  private async _tables(): Promise<Rljson> {
    throw new Error('Not implemented');
  }

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
    const tablesQuery = `
      SELECT name
      FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%';
    `;
    const returnFile: Rljson = {};
    const tables = this._db.prepare(tablesQuery).all();

    for (const table of tables as { name: string }[]) {
      const tableDump: Rljson = await this._dumpTable({ table: table.name });
      const tableDumpJson = JSON.parse(JSON.stringify(tableDump));
      returnFile[table.name] = tableDumpJson;
    }

    return returnFile;
  }

  // ...........................................................................
  private async _dumpTable(request: { table: string }): Promise<Rljson> {
    const query = `SELECT * FROM ${request.table}`;
    let returnFile: Rljson = {};
    const returnValue = this._db.prepare(query).all();
    returnFile = JSON.parse(JSON.stringify(returnValue));
    return returnFile;
  }

  // ...........................................................................
  private async _write(request: { data: Rljson }): Promise<void> {
    throw new Error('Not implemented ' + request);
  }
}

/* v8 ignore stop */
