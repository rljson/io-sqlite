<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Tools

- [Read architecture doc](#read-architecture-doc)
- [Rename classes](#rename-classes)
- [Create a new repo](#create-a-new-repo)
- [Handle issues](#handle-issues)

## Read architecture doc

Read [README.architecture.md](./README.architecture.md) to get an overview
of the package's architecture.

## Rename classes

Replace `ClassA` by `ClassB` in the following script and run it:

```bash
node ./scripts/rename-class.js ClassA ClassB
```

## Create a new repo

To create a new repo checkout [create-new-repo.md](doc/workflows/create-new-repo.md)

## Handle issues

Checkout [./README.trouble.md](./README.trouble.md)

Visit <https://github.com/rljson/io-sqlite/issues>

Check if there is already an issue for your problem.

If no, report the issue.

## Update goldens

In various tests test against golden files. To update these, execute:

```bash
pnpm updateGoldens
```

In vscode, click the `source control` icon at the left side bar

Click on changed golden files

Review the changes

On unwanted changes, fix the reason and update goldens again
