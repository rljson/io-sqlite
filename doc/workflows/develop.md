<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Ticket workflow

- [Checkout main](#checkout-main)
- [Define branch and PR name](#define-branch-and-pr-name)
- [Create a feature branch](#create-a-feature-branch)
- [Update dependencies](#update-dependencies)
- [Develop and debug with Vscode](#develop-and-debug-with-vscode)
- [Update goldens](#update-goldens)
- [Commit](#commit)
- [Increase version](#increase-version)
- [Run tests and build](#run-tests-and-build)
- [Rebase main](#rebase-main)
- [Create a pull request](#create-a-pull-request)
- [Code review](#code-review)
- [Delete feature branch](#delete-feature-branch)
- [Publish to NPM](#publish-to-npm)

## Checkout main

```bash
git checkout main
git fetch
git pull
```

## Define branch and PR name

In the _whole document_, replace the following things:

- `update-documentation-and-scripts` by the name of your new branch
- `Update documentation and scripts` by your new pull request title

## Create a feature branch

```bash
git checkout -b update-documentation-and-scripts
```

## Update dependencies

```bash
pnpm update --latest
```

## Develop and debug with Vscode

In Vscode: At the `left side bar` click on the `Test tube` icon to open the `Test explorer`

At the `top`, click on the `refresh` icon to show update the tests

Open a test file (`*.spec.ts`)

Set a breakpoint

Press `alt` and click on the play button left beside the test

Execution should stop at the breakpoint

## Commit

Use Vscode or another git client to commit your changes

## Increase version

```bash
pnpm version patch --no-git-tag-version
git commit -am"Increase version"
```

## Run tests and build

```bash
npm run build
```

## Rebase main

```bash
git rebase main
```

## Create a pull request

```bash
git push -u origin update-documentation-and-scripts
gh pr create --base main --title "Update documentation and scripts" --body " "
gh pr merge --auto --squash
```

## Code review

Read [setup-code-review.md](./code-review.md) on how to create a
code review.

## Delete feature branch

```bash
git fetch
git checkout main
git reset --soft origin/main
git stash -m"PR Aftermath"
git pull
git branch -d update-documentation-and-scripts
```

## Publish to NPM

```bash
npm publish --access public
node scripts/add-version-tag.js
```
