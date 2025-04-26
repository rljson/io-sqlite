// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io, IoTools } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { JsonValue, JsonValueType } from '@rljson/json';
import {
  ContentType,
  iterateTables,
  Rljson,
  TableCfg,
  TableType,
} from '@rljson/rljson';

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'fs';
import { mkdtemp, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

import { SqlStandards } from './sql-standards.ts';
import { DsSqliteStandards } from './sqlite-standards.ts';

type DBType = Database.Database;
const _sql = new DsSqliteStandards();

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
    this._init();
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
    const result = this._db.prepare(_sql.currentCount(table)).get() as {
      'COUNT(*)': number;
    };
    return result['COUNT(*)'];
  }

  // ...........................................................................
  async tableCfgs(): Promise<Rljson> {
    const result: Rljson = {};
    const returnValue = this._db.prepare(_sql.currentTableCfgs).all();
    const data = JSON.parse(JSON.stringify(returnValue)) as TableCfg[];
    const ownCfg = data.find((cfg) => cfg.key === 'tableCfgs') as TableCfg;

    result.tableCfgs = {
      _data: data,
      _type: 'ingredients',
      _tableCfg: ownCfg._hash as string,
    };
    return result;
  }

  private async _tableCfg(tableName: string): Promise<TableCfg> {
    const returnValue = this._db
      .prepare(_sql.currentTableCfg)
      .get(SqlStandards.removeTablePostFix(tableName)) as any;
    const returnCfg = JSON.parse(returnValue.tableCfg) as TableCfg;

    return returnCfg;
  }

  async allTableNames(): Promise<string[]> {
    const query = _sql.tableNames;
    const returnValue = this._db.prepare(query).all();
    const tableNames = (returnValue as { name: string }[]).map((row) =>
      SqlStandards.removeTablePostFix(row.name),
    );
    return tableNames;
  }

  // ...........................................................................
  // Write data into the respective table
  async write(request: { data: Rljson }): Promise<void> {
    await this._write(request);
  }

  createOrExtendTable(request: { tableCfg: TableCfg }): Promise<void> {
    return this._createTable(request);
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
    const tableCfg: TableCfg = {
      version: 1,
      key: 'tableCfgs',
      type: 'ingredients',
      isHead: true,
      isRoot: true,
      isShared: false,
      columns: [
        { key: 'version', type: 'number' },
        { key: 'key', type: 'string' },
        { key: 'type', type: 'string' },
        { key: 'tableCfg', type: 'json' },
      ],
    };

    hip(tableCfg);

    const tableCfgHashed = hsh(tableCfg);

    //create main table if it does not exist yet
    this._db.prepare(_sql.createMainTable).run();

    // Write tableCfg as first row into tableCfgs tableso
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
      this._db
        .prepare(_sql.insertTableCfg)
        .run(
          tableCfgHashed._hash,
          request.tableCfg.version,
          request.tableCfg.key,
          request.tableCfg.type,
          JSON.stringify(request.tableCfg),
        );
    } else {
      throw new Error(`Table ${request.tableCfg.key} already exists`);
    }

    // Create actual table with name from tableCfg
    const tableName = SqlStandards.addTablePostFix(request.tableCfg.key);
    const columnsCfg = request.tableCfg.columns;
    const columns = columnsCfg.map((col) => {
      const sqliteType = _sql.dataType(col.type);
      return `${SqlStandards.addColumnPostFix(col.key)} ${sqliteType}`;
    });

    const columnsString = columns.join(', ');
    this._db.exec(_sql.createTable(tableName, columnsString));
  }

  // ...........................................................................
  private async _readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    const tableKeyWithPostFix = SqlStandards.addTablePostFix(request.table);
    if (!this._tableExists(tableKeyWithPostFix)) {
      throw new Error(`Table ${request.table} does not exist`);
    }

    const tableCfg = await this._tableCfg(request.table);
    const columnTypes = tableCfg.columns.map((col) => col.type);
    const columnKeys = tableCfg.columns.map((col) => col.key);

    const columnsResult = this._db
      .prepare(_sql.columnKeys(tableKeyWithPostFix))
      .all() as { name: string }[];
    const columns = columnsResult.map((row) => row.name);
    const columnNamesSql = await this._returnColumns(columns);

    const whereString = this._whereString(Object.entries(request.where));
    const query = _sql.selection(
      tableKeyWithPostFix,
      columnNamesSql,
      whereString,
    );
    const returnValue = this._db.prepare(query).all() as {
      [key: string]: any;
    }[];

    const convertedResult: { [key: string]: any }[] = [];
    for (const row of returnValue) {
      const convertedRow: { [key: string]: any } = {};
      for (let colNum = 0; colNum < columns.length; colNum++) {
        const key = columnKeys[colNum];
        const type = columnTypes[colNum] as JsonValueType;
        const val = row[key];
        switch (type) {
          case 'boolean':
            convertedRow[key] = val !== 0;
            break;
          case 'null':
            convertedRow[key] = null as any;
            break;
          case 'jsonArray':
          case 'json':
            convertedRow[key] = JSON.parse(val);
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

    const result: Rljson = {
      [request.table]: {
        _data: convertedResult,
      },
    } as any;

    return result;
  }

  // ...........................................................................

  private async _dump(): Promise<Rljson> {
    const returnFile: Rljson = {};
    const tables = this._db.prepare(_sql.tableNames).all();

    for (const table of tables as { name: string }[]) {
      const tableDump: Rljson = await this._dumpTable({ table: table.name });

      returnFile[SqlStandards.removeTablePostFix(table.name)] =
        tableDump[SqlStandards.removeTablePostFix(table.name)];
    }

    return returnFile;
  }

  // ...........................................................................
  private async _dumpTable(request: { table: string }): Promise<Rljson> {
    const tableKeyWithPostFix = request.table.endsWith(
      SqlStandards.tablePostFix,
    )
      ? request.table
      : SqlStandards.addTablePostFix(request.table);

    // get table's column structure
    const tableCfg = await this._tableCfg(request.table);
    const columnTypes = tableCfg.columns.map((col) => col.type);
    const columnKeys = tableCfg.columns.map((col) => col.key);
    const columnKeysWithPostFix = columnKeys.map((col) =>
      SqlStandards.addColumnPostFix(col),
    );

    const returnFile: Rljson = {};
    let returnData;
    try {
      returnData = this._db
        .prepare(
          _sql.allData(tableKeyWithPostFix, columnKeysWithPostFix.join(', ')),
        )
        .all();
    } catch (error) {
      throw new Error(`Failed to dump table ${request.table} ` + error);
    }

    for (let i = 0; i < columnTypes.length; i++) {
      const columnKey = columnKeysWithPostFix[i];
      if (columnTypes[i] === 'json') {
        for (const row of returnData) {
          const typedRow = row as {
            [key: string]: any;
          };
          if (typedRow[columnKey] !== null) {
            const rowUnparsed = typedRow[columnKey];
            const rowParsed = JSON.parse(rowUnparsed);
            typedRow[columnKey] = rowParsed;
          }
        }
      }
    }

    const tableCfgHash = 'aa';
    const generalHash = 'aad';
    const tableType = (await this._tableType(request.table)) as ContentType;
    const table: TableType = {
      _data: returnData as any,
      _type: tableType,
      _tableCfg: tableCfgHash,
      _hash: generalHash,
    };
    returnFile[SqlStandards.removeTablePostFix(request.table)] = table;

    return returnFile;
  }

  // ...........................................................................
  private async _write(request: { data: Rljson }): Promise<void> {
    // Preparation
    const hashedData = hsh(request.data);
    const errorStore = new Map<number, string>();
    let errorCount = 0;

    // Loop through the tables in the data
    await iterateTables(hashedData, async (tableName, tableData) => {
      const tableCfg = await this._tableCfg(tableName);
      const columnTypes = tableCfg.columns.map((col) => col.type);
      const columnKeys = tableCfg.columns.map((col) => col.key);

      // Create internal table name
      const tableKeyWithPostFix = SqlStandards.addTablePostFix(tableName);

      // Check if table exists
      if (!this._tableExists(tableKeyWithPostFix)) {
        errorCount++;
        errorStore.set(errorCount, `Table ${tableName} does not exist`);
        return;
      }

      // Check if table type is correct
      const tableType = request.data[tableName]._type.toString();
      if (!this._tableTypeCheck(tableName, tableType)) {
        errorCount++;
        errorStore.set(
          errorCount,
          `Table type check failed for table ${tableName}, ${tableType}`,
        );
        return;
      }

      for (const row of tableData._data) {
        // Prepare and run the SQL query
        // (each row might have a different number of columns)
        const columnKeys = Object.keys(row);
        const columnKeysWithPostfix = columnKeys.map((column) =>
          SqlStandards.addColumnPostFix(column),
        );
        const placeholders = columnKeys.map(() => '?').join(', ');
        const query = `INSERT INTO ${tableKeyWithPostFix} (${columnKeysWithPostfix.join(
          ', ',
        )}) VALUES (${placeholders})`;

        // Put values into the necessary format
        const rowValues = this._valueList(columnKeys, columnTypes, row);

        // Run the query
        try {
          this._db.prepare(query).run(rowValues);
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : 'Unknown error';
          const fixedErrorMessage = errorMessage
            .replace(SqlStandards.columnPostFix, '')
            .replace(SqlStandards.tablePostFix, '');

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
    const tableKeyWithPostFix = tableKey.endsWith(SqlStandards.tablePostFix)
      ? tableKey
      : SqlStandards.addTablePostFix(tableKey);
    /* v8 ignore end */
    const result = this._db
      .prepare(_sql.tableExists)
      .get(tableKeyWithPostFix) as {
      count: number;
    };
    return result ? true : false;
  }

  _tableTypeCheck(tableName: string, tableType: string): boolean {
    const tableKey = SqlStandards.addColumnPostFix('type');

    const result = this._db
      .prepare(_sql.tableTypeCheck)
      .get(tableName) as Record<string, string>;
    return tableType === result[tableKey] ? true : false;
  }

  _valueList(columnKeys: string[], columnTypes: string[], row: any): any[] {
    const valueList: any[] = [];

    for (let i = 0; i < columnKeys.length; i++) {
      const key = columnKeys[i];
      const type = columnTypes[i];
      const val = row[key];

      switch (columnTypes[i]) {
        case 'string':
          valueList.push(val);
          break;
        case 'number':
          valueList.push(Number(val));
          break;
        case 'boolean':
          valueList.push(val ? 1 : 0);
          break;
        case 'object':
          valueList.push(JSON.stringify(val));
          break;
        case 'null':
          valueList.push(null);
          break;
        case 'jsonArray':
          valueList.push(JSON.stringify(val));
          break;
        case 'json':
          valueList.push(JSON.stringify(val));
          break;
        case undefined:
          valueList.push(`${val}`);
          break;
        default:
          throw new Error(`Unsupported column type ${type}`);
      }
    }

    return valueList;
  }

  async _returnColumns(columns: string[]): Promise<string> {
    const columnNames: string[] = [];
    for (const column of columns) {
      columnNames.push(
        `${column} AS [${SqlStandards.removeColumnPostFix(column)}]`,
      );
    }

    return columnNames.join(', ');
  }

  _whereString(whereClause: [string, JsonValue][]): string {
    let whereString: string = ' ';
    for (const [column, value] of whereClause) {
      const columnWithFix = SqlStandards.addColumnPostFix(column);

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
    const tableCfg = await this._tableCfg(
      SqlStandards.removeTablePostFix(tableName),
    );
    return tableCfg.type;
  }
}
