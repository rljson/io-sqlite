// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
import { expect, suite, test } from 'vitest';

import { DsSqliteStandards } from '../src/sqlite-standards';


// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

suite('DsSqliteStandards', () => {
  const dsSqliteStandards = new DsSqliteStandards();

  test('tableName generates correct query', () => {
    const expectedQuery = `SELECT name FROM sqlite_master WHERE type='table' AND name=?`;
    expect(dsSqliteStandards.tableName).toBe(expectedQuery);
  });

  test('tableNames generates correct query', () => {
    const expectedQuery = `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
    expect(dsSqliteStandards.tableNames).toBe(expectedQuery);
  });

  test('foreignKeyList generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `PRAGMA foreign_key_list(${tableName})`;
    expect(dsSqliteStandards.foreignKeyList(tableName)).toBe(expectedQuery);
  });

  test('allColumns generates correct query', () => {
    const tableName = 'testTable';
    const expectedQuery = `PRAGMA table_info(${tableName})`;
    expect(dsSqliteStandards.allColumnNames(tableName)).toBe(expectedQuery);
  });

  test('tableReferences generates correct query', () => {
    const referenceArray = ['col1', 'col2'];
    const expectedQuery = `FOREIGN KEY (col1) REFERENCES c (_hash), FOREIGN KEY (col2) REFERENCES c (_hash)`;
    expect(dsSqliteStandards.tableReferences(referenceArray)).toBe(
      expectedQuery,
    );
  });
  test('allTableNames generates correct query', () => {
    const expectedQuery = `SELECT name FROM sqlite_master WHERE type='table' and name NOT LIKE 'sqlite_%'`;
    expect(dsSqliteStandards.allTableNames()).toBe(expectedQuery);
  });
});
