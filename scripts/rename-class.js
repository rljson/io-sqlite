#!/bin/node

import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

if (process.argv.length < 4) {
  console.error(`Usage: ${path.basename(process.argv[1])} <CLASS_A> <CLASS_B>`);
  process.exit(1);
}

const CLASS_A = process.argv[2];
const CLASS_B = process.argv[3];

const toSnakeCase = (str) => {
  return str.replace(/(?<=[a-z0-9])([A-Z])/g, '-$1').toLowerCase();
};

const toLowerFirst = (str) => {
  return str.charAt(0).toLowerCase() + str.slice(1);
};

const LOWER_CLASS_A = toLowerFirst(CLASS_A);
const LOWER_CLASS_B = toLowerFirst(CLASS_B);
const SNAKE_CLASS_A = toSnakeCase(CLASS_A);
const SNAKE_CLASS_B = toSnakeCase(CLASS_B);

const replaceIncludesFirst = (directory) => {
  const files = fs.readdirSync(directory, { withFileTypes: true });

  for (const file of files) {
    const fullPath = path.join(directory, file.name);

    if (file.isDirectory()) {
      if (file.name.startsWith('.') || file.name === 'node_modules') continue;
      replaceIncludesFirst(fullPath);
    } else if (/(\.ts|\.md|\.js|\.json)$/.test(file.name)) {
      let content = fs.readFileSync(fullPath, 'utf8');
      content = content.replace(
        new RegExp(`(.*)${SNAKE_CLASS_A}(\\.ts)?([\\.\\/\\'\\\"\\\\])`, 'g'),
        `$1${SNAKE_CLASS_B}$2$3`,
      );
      fs.writeFileSync(fullPath, content, 'utf8');
    }
  }
};

const replaceInFiles = (directory) => {
  const files = fs.readdirSync(directory, { withFileTypes: true });

  for (const file of files) {
    const fullPath = path.join(directory, file.name);

    if (file.isDirectory()) {
      if (file.name.startsWith('.') || file.name === 'node_modules') continue;
      replaceInFiles(fullPath);
    } else if (/(\.ts|\.md|\.js|\.json)$/.test(file.name)) {
      let content = fs.readFileSync(fullPath, 'utf8');
      content = content
        .replace(new RegExp(CLASS_A, 'g'), CLASS_B)
        .replace(new RegExp(LOWER_CLASS_A, 'g'), LOWER_CLASS_B)
        .replace(new RegExp(SNAKE_CLASS_A, 'g'), SNAKE_CLASS_B);
      fs.writeFileSync(fullPath, content, 'utf8');
    }
  }
};

const renameFiles = (directory) => {
  const files = fs.readdirSync(directory, { withFileTypes: true });

  for (const file of files) {
    const fullPath = path.join(directory, file.name);

    if (file.isDirectory()) {
      if (file.name.startsWith('.') || file.name === 'node_modules') continue;
      renameFiles(fullPath);
    } else if (file.name.includes(SNAKE_CLASS_A)) {
      const newFileName = file.name.replace(SNAKE_CLASS_A, SNAKE_CLASS_B);
      fs.renameSync(fullPath, path.join(directory, newFileName));
    }
  }
};

console.log('Replacing includes first...');
replaceIncludesFirst('.');

console.log('Replacing occurrences in files...');
replaceInFiles('.');

console.log('Renaming files...');
renameFiles('.');

console.log('Updating goldens...');
fs.rmSync('test/goldens', { recursive: true, force: true });
execSync('pnpm updateGoldens', { stdio: 'inherit' });
