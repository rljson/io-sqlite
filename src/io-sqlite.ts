// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io, IoTools } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { Json, JsonValue, JsonValueType } from '@rljson/json';
import {
  ColumnCfg,
  ContentType,
  iterateTables,
  Rljson,
  TableCfg,
  TableKey,
  TableType,
} from '@rljson/rljson';

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'fs';
import { mkdtemp, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

import { SqlStatements as sql } from './sql-statements.ts';

type DBType = Database.Database;

export const exampleDbDir = join(tmpdir(), 'io-sqlite-tests');

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
  }

  async init(): Promise<void> {
    await this._init();
  }

  // Returns an example database directory
  static exampleDbDir = async (dbDir: string | undefined = undefined) => {
    // If dbDir is given, use it
    let newTempDir = '';
    if (dbDir) {
      const dir = join(tmpdir(), dbDir);
      if (!existsSync(dir)) {
        mkdirSync(dir);
      }
      newTempDir = dir;
    }
    // If no dbDir is given, create a new temp dir
    else {
      const prefix = join(tmpdir(), 'io-sqlite-'); // prefix must end with '-'
      newTempDir = await mkdtemp(prefix);
    }

    return newTempDir;
  };

  // Returns an example database file
  static exampleDbFilePath = async (dbDir: string | undefined = undefined) => {
    return join(await this.exampleDbDir(dbDir), 'example.sqlite');
  };

  /**
   * Returns an example database
   * @param dbDir - The directory to store the database file.
   * If not provided, a temporary directory will be created.
   */
  static example = async (dbDir: string | undefined = undefined) => {
    const tmpDb = await this.exampleDbFilePath(dbDir);
    return new IoSqlite(tmpDb);
  };

  async deleteDatabase() {
    this._db.close();
    await rm(this._dbPath as string);
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
  readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    return this._readRows(request);
  }

  async rowCount(table: string): Promise<number> {
    await this._ioTools.throwWhenTableDoesNotExist(table);

    const result = this._db.prepare(sql.rowCount(table)).get() as {
      'COUNT(*)': number;
    };
    return result['COUNT(*)'];
  }

  // ...........................................................................
  async tableExists(tableKey: TableKey): Promise<boolean> {
    return this._tableExists(tableKey);
  }

  // ...........................................................................
  async tableCfgs(): Promise<Rljson> {
    const result: Rljson = {};
    const tableCfg = IoTools.tableCfgsTableCfg;

    const returnValue = this._db.prepare(sql.tableCfgs).all() as Json[];
    const parsedReturnValue = this._parseData(returnValue, tableCfg);

    const ownCfg = parsedReturnValue.find(
      (cfg) => cfg.key === 'tableCfgs',
    ) as TableCfg;

    result.tableCfgs = {
      _data: parsedReturnValue,
      _type: 'ingredients',
      _tableCfg: ownCfg._hash as string,
    };
    return result;
  }

  // ...........................................................................
  private async _tableCfg(tableName: string): Promise<TableCfg> {
    const returnValue = this._db.prepare(sql.tableCfg).get(tableName) as any;
    const returnCfg = this._parseData(
      [returnValue],
      IoTools.tableCfgsTableCfg,
    ) as TableCfg[];

    return returnCfg[0];
  }

  async alltableKeys(): Promise<string[]> {
    const returnValue = this._db.prepare(sql.tableKeys).all();
    const tableKeys = (returnValue as { name: string }[]).map((row) =>
      sql.removeTableSuffix(row.name),
    );
    return tableKeys;
  }

  // ...........................................................................
  // Write data into the respective table
  async write(request: { data: Rljson }): Promise<void> {
    await this._write(request);
  }

  createOrExtendTable(request: { tableCfg: TableCfg }): Promise<void> {
    return this._createOrExtendTable(request);
  }

  // ######################
  // Private
  // ######################

  private _isReady = new IsReady();
  private _ioTools!: IoTools;

  // ...........................................................................
  private async _init() {
    // Create tableCfgs table
    this._ioTools = new IoTools(this);
    this._initTableCfgs();
    await this._ioTools.initRevisionsTable();
    this._isReady.resolve();
  }

  // ...........................................................................
  private _initTableCfgs = () => {
    const tableCfg = IoTools.tableCfgsTableCfg;

    //create main table if it does not exist yet
    this._db.prepare(sql.createTable(tableCfg)).run();

    // Write tableCfg as first row into tableCfgs tableso
    // As this is the first row to be entered, it is entered manually

    const values = this._serializeRow(tableCfg, tableCfg);

    try {
      const p = this._db.prepare(sql.insertTableCfg());
      p.run(...values);
    } catch (error) {
      throw new Error(
        `Failed to create tableCfgs table: ${error} - ${JSON.stringify(
          tableCfg,
        )}`,
      );
    }
  };

  // ...........................................................................
  private async _createOrExtendTable(request: {
    tableCfg: TableCfg;
  }): Promise<void> {
    // Make sure that the table config is compatible
    await this._ioTools.throwWhenTableIsNotCompatible(request.tableCfg);

    // Create table in sqlite database
    const tableKey = request.tableCfg.key;

    // Create config hash
    const tableCfgHashed = hsh(request.tableCfg);

    // Check if table exists
    const exists = this._db.prepare(sql.tableCfg).get(tableKey);

    if (!exists) {
      this._createTable(tableCfgHashed, request);
    } else {
      this._extendTable(tableCfgHashed);
    }
  }

  // ...........................................................................
  private _createTable(
    tableCfgHashed: TableCfg,
    request: { tableCfg: TableCfg },
  ) {
    try {
      this._insertTableCfg(tableCfgHashed);
      this._db.exec(sql.createTable(request.tableCfg));
    } catch (error) {
      throw new Error(
        `Failed to create tableCfgs table: ${error} - ${JSON.stringify(
          request.tableCfg,
        )}`,
      );
    }
  }

  // ...........................................................................
  private _insertTableCfg(tableCfgHashed: TableCfg) {
    hip(tableCfgHashed);
    const values = this._serializeRow(
      tableCfgHashed,
      IoTools.tableCfgsTableCfg,
    );
    this._db.prepare(sql.insertTableCfg()).run(...values);
  }

  // ...........................................................................
  private async _extendTable(newTableCfg: TableCfg): Promise<void> {
    // Estimate added columns
    const tableKey = newTableCfg.key;
    const oldTableCfg = await this._ioTools.tableCfg(tableKey);

    const addedColumns: ColumnCfg[] = [];
    for (
      let i = oldTableCfg.columns.length;
      i < newTableCfg.columns.length;
      i++
    ) {
      const newColumn = newTableCfg.columns[i];
      addedColumns.push(newColumn);
    }

    // No columns added? Do nothing.
    if (addedColumns.length === 0) {
      return;
    }

    // Write new tableCfg into tableCfgs table
    this._insertTableCfg(newTableCfg);

    // Add new columns to the table
    const alter = sql.alterTable(tableKey, addedColumns);
    this._db.prepare(alter).run();
  }

  // ...........................................................................
  private async _readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    await this._ioTools.throwWhenTableDoesNotExist(request.table);

    const tableKeyWithSuffix = sql.addTableSuffix(request.table);
    if (!this._tableExists(tableKeyWithSuffix)) {
      throw new Error(`Table ${request.table} not found`);
    }

    const tableCfg = await this._tableCfg(request.table);

    const whereString = this._whereString(Object.entries(request.where));
    const query = sql.selection(tableKeyWithSuffix, '*', whereString);
    const returnValue = this._db.prepare(query).all() as {
      [key: string]: any;
    }[];
    const convertedResult = this._parseData(returnValue, tableCfg);

    const result: Rljson = {
      [request.table]: {
        _data: convertedResult,
      },
    } as any;

    return result;
  }

  // ...........................................................................
  private _serializeRow(
    rowAsJson: Json,
    tableCfg: TableCfg,
  ): (JsonValue | null)[] {
    const result: (JsonValue | null)[] = [];

    // Iterate all columns in the tableCfg
    for (const col of tableCfg.columns) {
      const key = col.key;
      let value = rowAsJson[key] ?? null;
      const valueType = typeof value;

      // Stringify objects and arrays
      if (value !== null && valueType === 'object') {
        value = JSON.stringify(value);
      }

      // Convert booleans to 1 or 0
      else if (valueType === 'boolean') {
        value = value ? 1 : 0;
      }

      result.push(value);
    }

    return result;
  }

  private _parseData(data: Json[], tableCfg: TableCfg): Json[] {
    const columnTypes = tableCfg.columns.map((col) => col.type);
    const columnKeys = tableCfg.columns.map((col) => col.key);

    const convertedResult: Json[] = [];

    for (const row of data) {
      const convertedRow: { [key: string]: any } = {};
      for (let colNum = 0; colNum < columnKeys.length; colNum++) {
        const key = columnKeys[colNum];
        const keyWithSuffix = sql.addColumnSuffix(key);
        const type = columnTypes[colNum] as JsonValueType;
        const val = row[keyWithSuffix];

        // Null or undefined values are ignored
        // and not added to the converted row
        if (val === undefined) {
          continue;
        }

        if (val === null) {
          continue;
        }

        switch (type) {
          case 'boolean':
            convertedRow[key] = val !== 0;
            break;
          case 'jsonArray':
          case 'json':
            convertedRow[key] = JSON.parse(val as string);
            break;
          case undefined:
            convertedRow[key] = val;
            break;
          case 'string':
          case 'number':
            convertedRow[key] = val;
            break;
          /* v8 ignore start */
          default:
            throw new Error('Unsupported column type ' + type);
          /* v8 ignore end */
        }
      }

      convertedResult.push(convertedRow);
    }

    return convertedResult;
  }

  // ...........................................................................

  private async _dump(): Promise<Rljson> {
    const returnFile: Rljson = {};
    const tables = this._db.prepare(sql.tableKeys).all();

    for (const table of tables as { name: string }[]) {
      const tableDump: Rljson = await this._dumpTable({
        table: sql.removeTableSuffix(table.name),
      });

      returnFile[sql.removeTableSuffix(table.name)] =
        tableDump[sql.removeTableSuffix(table.name)];
    }

    return returnFile;
  }

  // ...........................................................................
  private async _dumpTable(request: { table: string }): Promise<Rljson> {
    await this._ioTools.throwWhenTableDoesNotExist(request.table);

    const tableKeyWithSuffix = sql.addTableSuffix(request.table);

    // get table's column structure
    const tableCfg = await this._tableCfg(request.table);
    const columnKeys = tableCfg.columns.map((col) => col.key);
    const columnKeysWithSuffix = columnKeys.map((col) =>
      sql.addColumnSuffix(col),
    );

    const returnFile: Rljson = {};
    let returnData: Json[];
    try {
      returnData = this._db
        .prepare(
          sql.allData(tableKeyWithSuffix, columnKeysWithSuffix.join(', ')),
        )
        .all() as Json[];
    } catch (error) {
      throw new Error(`Failed to dump table ${request.table} ` + error);
    }

    const parsedReturnData = this._parseData(returnData, tableCfg);

    const tableCfgHash = tableCfg._hash as string;
    const tableType = (await this._tableType(request.table)) as ContentType;
    const table: TableType = {
      _data: parsedReturnData as any,
      _type: tableType,
      _tableCfg: tableCfgHash,
      _hash: '',
    };
    returnFile[request.table] = table;

    return hip(returnFile, {
      throwOnWrongHashes: false,
      updateExistingHashes: false,
    });
  }

  // ...........................................................................
  private async _write(request: { data: Rljson }): Promise<void> {
    // Preparation
    const hashedData = hsh(request.data);
    const errorStore = new Map<number, string>();
    let errorCount = 0;

    await this._ioTools.throwWhenTablesDoNotExist(request.data);
    await this._ioTools.throwWhenTableDataDoesNotMatchCfg(request.data);

    // Loop through the tables in the data
    await iterateTables(hashedData, async (tableName, tableData) => {
      const tableCfg = await this._tableCfg(tableName);

      // Create internal table name
      const tableKeyWithSuffix = sql.addTableSuffix(tableName);

      // Check if table exists
      if (!this._tableExists(tableKeyWithSuffix)) {
        errorCount++;
        errorStore.set(errorCount, `Table ${tableName} does not exist`);
        return;
      }

      for (const row of tableData._data) {
        // Prepare and run the SQL query
        // (each row might have a different number of columns)
        const columnKeys = tableCfg.columns.map((col) => col.key);
        const columnKeysWithPostfix = columnKeys.map((column) =>
          sql.addColumnSuffix(column),
        );
        const placeholders = columnKeys.map(() => '?').join(', ');
        const query = `INSERT INTO ${tableKeyWithSuffix} (${columnKeysWithPostfix.join(
          ', ',
        )}) VALUES (${placeholders})`;

        // Put values into the necessary format
        const serializedRow = this._serializeRow(row, tableCfg);

        // Run the query
        try {
          this._db.prepare(query).run(serializedRow);
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : 'Unknown error';
          const fixedErrorMessage = errorMessage
            .replace(sql.suffix.col, '')
            .replace(sql.suffix.tbl, '');

          errorCount++;
          errorStore.set(
            errorCount,
            `Error inserting into table ${tableName}: ${fixedErrorMessage}`,
          );
        }
      }
    });

    if (errorCount > 0) {
      const errorMessages = Array.from(errorStore.values()).join(', ');
      throw new Error(`Errors occurred: ${errorMessages}`);
    }
  }

  _tableExists(tableKey: string): boolean {
    /* v8 ignore start */
    const tableKeyWithSuffix = sql.addTableSuffix(tableKey);
    /* v8 ignore end */
    const result = this._db
      .prepare(sql.tableExists)
      .get(tableKeyWithSuffix) as {
      count: number;
    };
    return result ? true : false;
  }

  _tableTypeCheck(tableName: string, tableType: string): boolean {
    const tableKey = sql.addColumnSuffix('type');

    const result = this._db
      .prepare(sql.tableTypeCheck)
      .get(tableName) as Record<string, string>;
    return tableType === result[tableKey] ? true : false;
  }

  async _returnColumns(
    columnKeysWithSuffix: string[],
    columnKeys: string[],
  ): Promise<string> {
    const columnNames: string[] = [];
    for (let i = 0; i < columnKeys.length; i++) {
      const key = columnKeys[i];
      const keyWithSuffix = columnKeysWithSuffix[i];
      columnNames.push(`${keyWithSuffix} AS [${key}]`);
    }

    return columnNames.join(', ');
  }

  _whereString(whereClause: [string, JsonValue][]): string {
    let whereString: string = ' ';
    for (const [column, value] of whereClause) {
      const columnWithFix = sql.addColumnSuffix(column);

      if (typeof value === 'string') {
        whereString += `${columnWithFix} = '${value}' AND `;
      } else if (typeof value === 'number') {
        whereString += `${columnWithFix} = ${value} AND `;
      } else if (typeof value === 'boolean') {
        whereString += `${columnWithFix} = ${value ? 1 : 0} AND `;
      } else if (value === null) {
        whereString += `${columnWithFix} IS NULL AND `;
      } else if (typeof value === 'object') {
        whereString += `${columnWithFix} = '${JSON.stringify(value)}' AND `;
      } else {
        throw new Error(`Unsupported value type for column ${column}`);
      }
    }

    whereString = whereString.endsWith('AND ')
      ? whereString.slice(0, -5)
      : whereString; // remove last ' AND '

    return whereString;
  }

  async _tableType(tableName: string): Promise<string> {
    const tableCfg = await this._tableCfg(sql.removeTableSuffix(tableName));
    return tableCfg.type;
  }

  async _mutualColumns(tableKey: string): Promise<string> {
    const tempColumns = this._db.prepare(sql.columnKeys(tableKey)).all();
    // const newColumns = this._db
    //   .prepare(SQL.columnKeys(tableKey + SQL.fix.tbl))
    //   .all();

    const mutualColumns = tempColumns;
    //   .split('\n')
    //   .filter((col) => newColumns.split('\n').includes(col))
    //   .map((col) => col.trim());

    return mutualColumns.join(', ');
  }
}
