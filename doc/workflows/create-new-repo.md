<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Create a new Rljson repository

## Replace in this doc

Replace `my-new-repo` by the name of your new repo

Replace `MyNewRepo` by the name of your new repo

## Create repo

Open <https://github.com/rljson>

Select `Repositories`

Click `New repository`

Below `Repository template` click on the drop down `No template`

Select the desired template repository

Create the repo as used

## Setup branch rules

<https://stackoverflow.com/a/57685576/1210942>

Open <https://github.com/rljson/my-new-repo>

Click `Settings`

Click `Branches`

Locate `Branch Protection Rules`

Click `Add branch ruleset`

Enter the following values:

| Key                | Value   |
| ------------------ | ------- |
| Ruleset Name       | Default |
| Enforcement status | Active  |

Locate `Branch targeting criteria`

Click `Add target`

Select `Include default branch`

Check the following settings:

- [x] `Restrict deletions`
- [x] `Require linear history`
- [x] `Require a pull request before merging`
  - [x] `Allowed merge methods:`: `Squash`
- [x] `Require status checks to pass`
  - [x] `Require branches to be up to date before merging`
  - Click `Add checks`
  - Enter `Build` into the search field
  - Select `Build and Test` GitHub Actions
- [x] `Block force pushes`

Click `Create`

Authenticate

## Require deleting branches after merge

Open <https://github.com/rljson/my-new-repo>

Click `Settings`

Scroll down to `Pull Requests`

Apply the following settings:

- [ ] `Allow merge commits`
- [x] `Allow squash merging`
- [ ] `Allow rebase merging`
- [x] `Alway suggest updating pull request branches`
- [x] `Allow auto-merge`
- [x] `Automatically delete head branches`

## Prepare project

Checkout the project

```bash
cd ~/dev/rljson
git clone git@github.com:rljson/my-new-repo.git
cd my-new-repo
pnpm install
pnpm build
```

Open project with vscode

```bash
code .
```

Prepare a new branch and pull request

```bash
git checkout -b extend-readmes-on-npm-publishing-and-repo-creation
```

## Rename template into my-new-repo

```bash
node scripts/rename-class.js template
```

## Update goldens

```bash
pnpm updateGoldens
```

## Commit the initial state

```bash
git add .
git commit -am "Rename template into MyNewRepo"
```

## Merge and publish

```bash
code `README.contributors.md`
```

Go to section `### Update dependencies` and follow the instruction
