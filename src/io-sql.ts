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
  RljsonTable,
  TableCfg,
  TableKey,
  TableType,
} from '@rljson/rljson';

import { existsSync, mkdirSync } from 'fs';
import { mkdtemp } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

import { SqlStatements } from './sql-statements.ts';

export const exampleDbDir = join(tmpdir(), 'io-sqlite-tests');

export interface DbType {
  prepare: (source: string) =>
    | {
        all: (...params: unknown[]) => unknown[];
        get: (...params: unknown[]) => unknown;
        run: (...params: unknown[]) => unknown;
      }
    | any;
  exec: (sql: string) => DbType;
  close: () => DbType;
  open: boolean;
}

/* v8 ignore start */
/**
 * Sqlite implementation of the Rljson Io interface.
 */
export class IoSql implements Io {
  // ...........................................................................
  // Constructor & example
  constructor(
    protected readonly _createDb: () => Promise<DbType>,
    public readonly sql: SqlStatements,
  ) {}
  async contentType(request: { table: string }): Promise<ContentType> {
    const query = this.sql.contentType(request.table);
    const result = this.db.prepare(query).all();
    const mappedResult = result.map((type_col: string) =>
      typeof type_col === 'string' ? type_col : JSON.stringify(type_col),
    );
    const obj = JSON.parse(mappedResult[0]);
    const type = obj.type_col; // "components"
    return type;
  }

  async init(): Promise<void> {
    this.db = await this._createDb();
    this._isOpen = true;
    await this._init();
  }

  public db!: DbType;

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

  // ...........................................................................
  // General
  isReady() {
    return this._isReady.promise;
  }

  private _isOpen = false;

  public get isOpen(): boolean {
    return this._isOpen;
  }

  async close() {
    try {
      this._isOpen = false;
      this.db.close();
    } catch (e) {
      // Ignore error
      console.log('Error closing database:', e);
    }
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

    const result = this.db.prepare(this.sql.rowCount(table)).get() as {
      'COUNT(*)': number;
    };
    return result['COUNT(*)'];
  }

  // ...........................................................................
  async tableExists(tableKey: TableKey): Promise<boolean> {
    return this._tableExists(tableKey);
  }
  // ...........................................................................
  async rawTableCfgs(): Promise<TableCfg[]> {
    const tableCfg = IoTools.tableCfgsTableCfg;
    const returnValue = this.db.prepare(this.sql.tableCfgs).all() as Json[];
    const parsedReturnValue = this._parseData(returnValue, tableCfg);
    return parsedReturnValue as TableCfg[];
  }

  async alltableKeys(): Promise<string[]> {
    const returnValue = this.db.prepare(this.sql.tableKeys).all();
    const tableKeys = (returnValue as { name: string }[]).map((row) =>
      this.sql.removeTableSuffix(row.name),
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
    this.db.prepare(this.sql.createTable(tableCfg)).run();

    // Write tableCfg as first row into tableCfgs tableso
    // As this is the first row to be entered, it is entered manually
    const values = this._serializeRow(tableCfg, tableCfg);

    const p = this.db.prepare(this.sql.insertTableCfg());
    p.run(...values);
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
    const exists = this.db.prepare(this.sql.tableCfg).all(tableKey);

    if (exists.length === 0) {
      this._createTable(tableCfgHashed, request);
    } else {
      await this._extendTable(tableCfgHashed);
    }
  }

  // ...........................................................................
  private _createTable(
    tableCfgHashed: TableCfg,
    request: { tableCfg: TableCfg },
  ) {
    this._insertTableCfg(tableCfgHashed);
    this.db.exec(this.sql.createTable(request.tableCfg));
  }

  // ...........................................................................
  private _insertTableCfg(tableCfgHashed: TableCfg) {
    hip(tableCfgHashed);
    const values = this._serializeRow(
      tableCfgHashed,
      IoTools.tableCfgsTableCfg,
    );
    this.db.prepare(this.sql.insertTableCfg()).run(...values);
  }

  // ...........................................................................
  _addMissingHashes(rljson: Json): void {
    hip(rljson, { updateExistingHashes: false, throwOnWrongHashes: false });
  }

  // ...........................................................................
  private async _extendTable(newTableCfg: TableCfg): Promise<void> {
    // Estimate added columns
    const tableKey = newTableCfg.key;
    try {
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
      const alter = this.sql.alterTable(tableKey, addedColumns);
      for (const statement of alter) {
        this.db.prepare(statement).run();
      }
    } catch (e) {
      console.error(e);
    }
  }

  // ...........................................................................
  private async _readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    await this._ioTools.throwWhenTableDoesNotExist(request.table);
    await this._ioTools.throwWhenColumnDoesNotExist(request.table, [
      ...Object.keys(request.where),
    ]);

    const tableKeyWithSuffix = this.sql.addTableSuffix(request.table);
    const tableCfg = await this._ioTools.tableCfg(request.table);

    const whereString = this._whereString(Object.entries(request.where));
    const query = this.sql.selection(tableKeyWithSuffix, '*', whereString);
    const returnValue = this.db.prepare(query).all() as {
      [key: string]: any;
    }[];
    const convertedResult = this._parseData(returnValue, tableCfg);

    const table: RljsonTable<any, any> = {
      _data: convertedResult,
      _type: tableCfg.type,
    };

    this._ioTools.sortTableDataAndUpdateHash(table);

    const result: Rljson = {
      [request.table]: table,
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
        const keyWithSuffix = this.sql.addColumnSuffix(key);
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

  public parseDataTest(data: Json[], tableCfg: TableCfg): Json[] {
    return this._parseData(data, tableCfg);
  }

  // ...........................................................................

  private async _dump(): Promise<Rljson> {
    const returnFile: Rljson = {};
    const tables = this.db.prepare(this.sql.tableKeys).all();

    for (const table of tables as { name: string }[]) {
      const tableDump: Rljson = await this._dumpTable({
        table: this.sql.removeTableSuffix(table.name),
      });

      returnFile[this.sql.removeTableSuffix(table.name)] =
        tableDump[this.sql.removeTableSuffix(table.name)];
    }

    this._addMissingHashes(returnFile);

    return returnFile;
  }

  // ...........................................................................
  private async _dumpTable(request: { table: string }): Promise<Rljson> {
    await this._ioTools.throwWhenTableDoesNotExist(request.table);

    const tableKeyWithSuffix = this.sql.addTableSuffix(request.table);

    // get table's column structure
    const tableCfg = await this._ioTools.tableCfg(request.table);
    const columnKeys = tableCfg.columns.map((col) => col.key);
    const columnKeysWithSuffix = columnKeys.map((col) =>
      this.sql.addColumnSuffix(col),
    );

    const returnFile: Rljson = {};
    let returnData: Json[];
    try {
      returnData = this.db
        .prepare(
          this.sql.allData(tableKeyWithSuffix, columnKeysWithSuffix.join(', ')),
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

    this._ioTools.sortTableDataAndUpdateHash(table);

    returnFile[request.table] = table;

    return returnFile;
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
      const tableCfg = await this._ioTools.tableCfg(tableName);

      // Create internal table name
      const tableKeyWithSuffix = this.sql.addTableSuffix(tableName);

      // // Check if table exists
      // if (!this._tableExists(tableKeyWithSuffix)) {
      //   errorCount++;
      //   errorStore.set(errorCount, `Table ${tableName} does not exist`);
      //   return;
      // }

      for (const row of tableData._data) {
        // Prepare and run the SQL query
        // (each row might have a different number of columns)
        const columnKeys = tableCfg.columns.map((col) => col.key);
        const columnKeysWithPostfix = columnKeys.map((column) =>
          this.sql.addColumnSuffix(column),
        );
        const placeholders = columnKeys.map(() => '?').join(', ');
        const query = `INSERT INTO ${tableKeyWithSuffix} (${columnKeysWithPostfix.join(
          ', ',
        )}) VALUES (${placeholders})`;

        // Put values into the necessary format
        const serializedRow = this._serializeRow(row, tableCfg);

        // Run the query
        try {
          this.db.prepare(query).run(serializedRow);
        } catch (error) {
          if ((error as any).code === 'SQLITE_CONSTRAINT_PRIMARYKEY') {
            return;
          }

          const errorMessage =
            error instanceof Error ? error.message : 'Unknown error';
          const fixedErrorMessage = errorMessage
            .replace(this.sql.suffix.col, '')
            .replace(this.sql.suffix.tbl, '');

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
    const tableKeyWithSuffix = this.sql.addTableSuffix(tableKey);
    /* v8 ignore end */
    const result = this.db
      .prepare(this.sql.tableExists)
      .get(tableKeyWithSuffix) as {
      count: number;
    };
    return result ? true : false;
  }

  _tableTypeCheck(tableName: string, tableType: string): boolean {
    const tableKey = this.sql.addColumnSuffix('type');

    const result = this.db
      .prepare(this.sql.tableTypeCheck)
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
      const columnWithFix = this.sql.addColumnSuffix(column);

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
    const tableCfg = await this._ioTools.tableCfg(
      this.sql.removeTableSuffix(tableName),
    );
    return tableCfg.type;
  }

  async _mutualColumns(tableKey: string): Promise<string> {
    const tempColumns = this.db.prepare(this.sql.columnKeys(tableKey)).all();
    // const newColumns = this._db
    //   .prepare(this.SQL.columnKeys(tableKey + this.SQL.fix.tbl))
    //   .all();

    const mutualColumns = tempColumns;
    //   .split('\n')
    //   .filter((col) => newColumns.split('\n').includes(col))
    //   .map((col) => col.trim());

    return mutualColumns.join(', ');
  }
}
