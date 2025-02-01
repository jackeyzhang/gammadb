# **Contributing to GammaDB**

Welcome! We're excited that you're interested in contributing to GammaDB and want to make the process as smooth as possible.

## Technical Info

Before submitting a pull request, please review this document, which outlines what
conventions to follow when submitting changes. If you have any questions not covered
in this document, please reach out to us in the [GammaDB Website](https://www.gammadb.com)
or via [email](mailto:jackey@gammadb.com).

### Claiming GitHub Issues

This repository has a workflow to automatically assign issues to new contributors. This ensures that you don't need approval
from a maintainer to pick an issue.

1. Before claiming an issue, ensure that:

- It's not already assigned to someone else
- There are no comments indicating ongoing work

2. To claim an unassigned issue, simply comment `/take` on the issue. This will automatically assign the issue to you.

If you find yourself unable to make progress, don't hesitate to seek help in the issue comments or in the [GammaDB Website](https://www.gammadb.com). If you no longer wish to
work on the issue(s) you self-assigned, please use the `unassign me` link at the top of the issue(s) page to release it.

### Development Workflow

For development instructions regarding a specific Postgres extension, please refer to the Development section of the README in the extension's subfolder.

The development of GammaDB, which is the combination of our Postgres extensions and of community Postgres extensions packaged together, is done via Docker.

### Pull Request Workflow

All changes to GammaDB happen through GitHub Pull Requests. Here is the recommended
flow for making a change:

1. Before working on a change, please check to see if there is already a GitHub issue open for that change.
2. If there is not, please open an issue first. This gives the community visibility into what you're working on and allows others to make suggestions and leave comments.
3. Fork the GammaDB repo and branch out from the `main` branch.
4. Make your changes. If you've added new functionality, please add tests. We will not merge a feature without appropriate tests.
5. Open a pull request towards the `main` branch. Ensure that all tests and checks pass. Note that the GammaDB repository has pull request title linting in place and follows the [Conventional Commits spec](https://github.com/amannn/action-semantic-pull-request).
6. Congratulations! Our team will review your pull request.

### Contributor License Agreement

In order for us, Gamma Data, Inc. (dba GammaDB) to accept patches and other contributions from you, you need to adopt our GammaDB Contributor License Agreement (the "**CLA**"). The current version of the CLA can be found [here](https://cla-assistant.io/gammadb/gammadb).

GammaDB uses a tool called CLA Assistant to help us keep track of the CLA status of contributors. CLA Assistant will post a comment to your pull request indicating whether you have signed the CLA or not. If you have not signed the CLA, you will need to do so before we can accept your contribution. Signing the CLA is a one-time process, is valid for all future contributions to GammaDB, and can be done in under a minute by signing in with your GitHub account.

If you have any questions about the CLA, please reach out to us via email at [jackey@gammadb.com](mailto:jackey@gammadb.com).

### License

By contributing to GammaDB, you agree that your contributions will be licensed under the [GNU Affero General Public License v3.0](LICENSE) and as commercial software.
