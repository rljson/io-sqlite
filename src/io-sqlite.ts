// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Io } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { JsonValue } from '@rljson/json';
import { Rljson, TableCfg, TableCfgRef } from '@rljson/rljson';

/* v8 ignore start */

/**
 * In-Memory implementation of the Rljson Io interface.
 */
export class IoSqlite implements Io {
  // ...........................................................................
  // Constructor & example
  constructor() {
    this._init();
  }

  static example = () => {
    return new IoSqlite();
  };

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

  async dumpTable(request: { table: string }): Promise<Rljson> {
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

  async tables(): Promise<string[]> {
    return this._tables();
  }

  // ######################
  // Private
  // ######################

  private _isReady = new IsReady();

  // ...........................................................................
  private async _init() {
    this._initTableCfgs();
    this._isReady.resolve();
  }

  // ...........................................................................
  private _initTableCfgs = () => {
    const tableCfg: TableCfg = {
      key: 'tableCfgs',
      type: 'properties',
      columns: {
        key: { key: 'key', type: 'string', previous: 'string' },
        type: { key: 'type', type: 'string', previous: 'string' },
      },
    };

    hip(tableCfg);

    // Todo: Write tableCfg as first row into tableCfgs tables
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
    // ....
  }

  // ...........................................................................
  private async _tables(): Promise<string[]> {
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
    throw new Error('Not implemented ');
  }

  // ...........................................................................
  private async _dumpTable(request: { table: string }): Promise<Rljson> {
    throw new Error('Not implemented ' + request);
  }

  // ...........................................................................
  private async _write(request: { data: Rljson }): Promise<void> {
    throw new Error('Not implemented ' + request);
  }
}

/* v8 ignore stop */
