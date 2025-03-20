/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

import { execSync } from 'child_process';

// Define red, blue, yellow methods
const red = (str) => `\x1b[31m${str}\x1b[0m`;
const blue = (str) => `\x1b[34m${str}\x1b[0m`;
const yellow = (str) => `\x1b[33m${str}\x1b[0m`;
const green = (str) => `\x1b[32m${str}\x1b[0m`;

function getPRUrl() {
  try {
    const json = execSync('gh pr view --json url', {
      encoding: 'utf-8',
    }).trim();

    const parsed = JSON.parse(json);
    const url = parsed.url;
    console.log(blue(url));
  } catch (error) {
    console.error(yellow('No PR available'));
    process.exit(1);
  }
}

function getPRStatus() {
  try {
    const jsonString = execSync('gh pr view --json state', {
      encoding: 'utf-8',
    }).trim();

    const jsonParsed = JSON.parse(jsonString);
    return jsonParsed.state;
  } catch (error) {
    console.error(red('Error fetching PR status'));
    process.exit(1);
  }
}

async function checkIfPipelineHasFailed() {
  try {
    const json = execSync(
      'gh run list --repo rljson/IoSqlite --limit 1 --json status,conclusion',
      {
        encoding: 'utf-8',
      },
    ).trim();

    const jsonParsed = JSON.parse(json);
    if (jsonParsed.length) {
      if (jsonParsed[0].conclusion === 'failure') {
        console.error(red('Pipeline has failed. Please fix.'));
        process.exit(1);
      }
    }
  } catch (e) {
    console.error(red('Error fetching pipeline status'));
    process.exit(1);
  }
}

async function waitForPRClosure() {
  console.log(yellow('Wait for pipelines, code review and merge ...'));

  while (true) {
    const status = getPRStatus();

    if (status === 'MERGED') {
      console.log(green('PR has been merged.'));
      break;
    } else if (status === 'CLOSED') {
      console.log(green('PR has been closed.'));
      break;
    } else if (status === 'FAILED') {
      console.log(red('Error: PR has failed.'));
      break;
    }

    await checkIfPipelineHasFailed();

    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

async function main() {
  getPRUrl();
  await waitForPRClosure();
}

main();
