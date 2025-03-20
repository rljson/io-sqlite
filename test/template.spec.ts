// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { Template } from '../src/template';


describe('Template', () => {
  it('should validate a template', () => {
    const template = Template.example;
    expect(template).toBeDefined();
  });
});
