// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { JsonValue } from '@rljson/json';
import { ContentType, iterateTables, Rljson, TableCfg, TableType } from '@rljson/rljson';

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'fs';
import { mkdtemp, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

import { IoInit } from './io-sqlite-init.ts';
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
    try {
      await this._write(request);
    } catch (error) {
      throw new Error(
        `${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  createTable(request: { tableCfg: TableCfg }): Promise<void> {
    return this._createTable(request);
  }

  // ######################
  // Private
  // ######################

  private _isReady = new IsReady();
  private _ioInit!: IoInit;

  // ...........................................................................
  private async _init() {
    // Create tableCfgs table
    this._ioInit = new IoInit(this);
    this._initTableCfgs();
    await this._ioInit.initRevisionsTable();
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
      columns: {
        version: { type: 'number' },
        key: { type: 'string' },
        type: { type: 'string' },
        tableCfg: { type: 'json' },
      },
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
    const columns = Object.keys(columnsCfg).flatMap((key) => {
      const column = columnsCfg[key];
      const sqliteType = _sql.dataType(column.type);
      return `${SqlStandards.addColumnPostFix(key)} ${sqliteType}`;
    });

    const columnsString = columns.join(', ');
    this._db.exec(_sql.createTable(tableName, columnsString));
  }

  // ...........................................................................
  private async _readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    const fixedTableName = SqlStandards.addTablePostFix(request.table);
    if (!this._tableExists(fixedTableName)) {
      throw new Error(`Table ${request.table} does not exist`);
    }

    const columnTypes: { [key: string]: string } = await this._columnTypes(
      request.table,
    );

    const columnsResult = this._db
      .prepare(_sql.columnNames(fixedTableName))
      .all() as { name: string }[];
    const columns = columnsResult.map((row) => row.name);
    const columnNames = await this._returnColumns(columns);

    const whereString = this._whereString(Object.entries(request.where));
    const query = _sql.selection(fixedTableName, columnNames, whereString);
    const returnValue = this._db.prepare(query).all() as {
      [key: string]: any;
    }[];

    const convertedResult: { [key: string]: any }[] = [];
    for (const row of returnValue) {
      const convertedRow: { [key: string]: any } = {};
      for (const column of columns) {
        const columnName = SqlStandards.removeColumnPostFix(column);
        switch (columnTypes[columnName]) {
          case 'boolean':
            convertedRow[columnName] = row[columnName] === 1 ? true : false;
            break;
          case 'null':
            convertedRow[columnName] = null as any;
            break;
          case 'object':
          case 'jsonArray':
          case 'json':
            convertedRow[columnName] = JSON.parse(row[columnName]);
            break;
          case undefined:
            convertedRow[columnName] = row[columnName];
            break;
          default:
            convertedRow[columnName] = row[columnName];
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
    const fixedTableName = request.table.endsWith(SqlStandards.tablePostFix)
      ? request.table
      : SqlStandards.addTablePostFix(request.table);
    const columnNames = this._db
      .prepare(_sql.columnNames(fixedTableName))
      .all()
      .map(
        (row) =>
          `${
            (row as { name: string }).name
          } AS [${SqlStandards.removeColumnPostFix(
            (row as { name: string }).name,
          )}]`,
      ) as string[];

    const returnFile: Rljson = {};
    let returnData;
    try {
      returnData = this._db
        .prepare(_sql.allData(fixedTableName, columnNames.join(', ')))
        .all();
    } catch (error) {
      throw new Error(`Failed to dump table ${request.table} ` + error);
    }

    // get table's column structure
    const columnTypes: { [key: string]: string } = await this._columnTypes(
      request.table,
    );
    for (const column of Object.keys(columnTypes)) {
      if (columnTypes[column] === 'json') {
        for (const row of returnData) {
          const typedRow: { [key: string]: any } = row as {
            [key: string]: any;
          };
          if (typedRow[column] !== null) {
            typedRow[column] = JSON.parse(
              (row as { [key: string]: any })[column],
            );
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
      // Create internal table name
      const fixedTableName = SqlStandards.addTablePostFix(tableName);

      // Check if table exists
      if (!this._tableExists(fixedTableName)) {
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

      const columnTypes = await this._columnTypes(tableName);

      for (const row of tableData._data) {
        // Prepare and run the SQL query
        // (each row might have a different number of columns)
        const columns = Object.keys(row);
        const fixedColumnNames = columns.map((column) =>
          SqlStandards.addColumnPostFix(column),
        );
        const placeholders = columns.map(() => '?').join(', ');
        const query = `INSERT INTO ${fixedTableName} (${fixedColumnNames.join(
          ', ',
        )}) VALUES (${placeholders})`;

        // Put values into the necessary format
        const rowValues = this._valueList(columns, columnTypes, row);

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

  public async allColumnNames(
    tableName: string,
  ): Promise<{ name: string; type: string }[]> {
    let actualTableName = tableName;
    if (tableName !== SqlStandards.mainTable) {
      actualTableName = tableName.endsWith(SqlStandards.tablePostFix)
        ? tableName
        : SqlStandards.addTablePostFix(tableName);
    }
    const result: { name: string; type: string }[] = [];
    const rows = this._db.prepare(_sql.columnNames(actualTableName)).all() as {
      name: string;
      type: string;
    }[];
    for (const row of rows) {
      result.push({
        name: SqlStandards.removeColumnPostFix(row.name),
        type: row.type,
      });
    }
    return result;
  }

  _tableExists(tableName: string): boolean {
    const fixedTableName = tableName.endsWith(SqlStandards.tablePostFix)
      ? tableName
      : SqlStandards.addTablePostFix(tableName);
    const result = this._db.prepare(_sql.tableExists).get(fixedTableName) as {
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

  _currentCount(tableName: string): number {
    const result = this._db.prepare(_sql.currentCount(tableName)).get() as {
      'COUNT(*)': number;
    };
    return result['COUNT(*)'];
  }

  _valueList(
    originalColumns: string[],
    columnTypes: { [key: string]: string },
    row: any,
  ): any[] {
    const valueList: any[] = [];

    for (const column of originalColumns) {
      switch (columnTypes[column]) {
        case 'string':
          valueList.push(row[column]);
          break;
        case 'number':
          valueList.push(Number(row[column]));
          break;
        case 'boolean':
          valueList.push(row[column] ? 1 : 0);
          break;
        case 'object':
          valueList.push(JSON.stringify(row[column]));
          break;
        case 'null':
          valueList.push(null);
          break;
        case 'jsonArray':
          valueList.push(JSON.stringify(row[column]));
          break;
        case 'json':
          valueList.push(JSON.stringify(row[column]));
          break;
        case undefined:
          valueList.push(`${row[column]}`);
          break;
        default:
          throw new Error(`Unsupported column type ${columnTypes[column]}`);
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

  async _columnTypes(tableName: string): Promise<{ [key: string]: string }> {
    const tableCfg = await this._tableCfg(tableName);
    const columnTypes: { [key: string]: string } = {};
    for (const [key, column] of Object.entries(tableCfg.columns)) {
      columnTypes[key] = column.type;
    }
    return columnTypes;
  }
  async _tableType(tableName: string): Promise<string> {
    const tableCfg = await this._tableCfg(
      SqlStandards.removeTablePostFix(tableName),
    );
    return tableCfg.type;
  }
}
