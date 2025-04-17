import { beforeEach, describe, expect, it, vi } from 'vitest';

import { IoSqlite } from '../src/io-sqlite';
import { IoInit } from '../src/io-sqlite-init';

// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
describe('IoInit', () => {
  let mockIoSqlite: IoSqlite;
  let ioInit: IoInit;

  beforeEach(() => {
    mockIoSqlite = {
      createTable: vi.fn(),
    } as unknown as IoSqlite;

    ioInit = new IoInit(mockIoSqlite);
  });

  describe('tableCfg', () => {
    it('should return the correct table configuration', () => {
      const tableCfg = ioInit.tableCfg;

      expect(tableCfg).toEqual({
        version: 1,
        key: 'tableCfgs',
        type: 'ingredients',
        isHead: false,
        isRoot: false,
        isShared: true,
        _hash: 'FnkfKIfksP8lVDEi1hjKBn',
        columns: {
          _hash: 'i9dW71QAqKTOtXrdHzedya',
          key: {
            _hash: 'AEBOaGQVNw8XEcTXrPopBU',
            type: 'string',
          },
          type: {
            _hash: 'AEBOaGQVNw8XEcTXrPopBU',
            type: 'string',
          },
        },
      });

      describe('initRevisionsTable', () => {
        it('should call createTable with the correct table configuration', async () => {
          await ioInit.initRevisionsTable();

          expect(mockIoSqlite.createTable).toHaveBeenCalledWith({
            tableCfg: {
              version: 1,
              key: 'revisions',
              type: 'ingredients',
              isHead: false,
              isRoot: false,
              isShared: true,
              columns: {
                table: { type: 'string' },
                predecessor: { type: 'string' },
                successor: { type: 'string' },
                timestamp: { type: 'number' },
                id: { type: 'string' },
              },
            },
          });
        });
      });
    });
  });
});
