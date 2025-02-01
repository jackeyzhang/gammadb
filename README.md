<h1 align="center">
  <a href="https://gammadb.com"><img width=200 src="logo.svg" alt="GammaDB"></a>
<br>
</h1>

<p align="center">
  <b>GammaDB: Native Analytical PostgreSQL</b> <br />
</p>


---

[GammaDB](https://gammadb.com): One-Click Transform PostgreSQL to Native Analytical Database.

GammaDB is a product recently developed by our team. It utilizes PostgreSQL's EXTENSION technology to implement:

- cost-based vectorized optimization
- vectorized execution
- columnar storage engine
- columnar index built on Heap tables. 

These innovative designs aim to significantly enhance the analytical performance of PostgreSQL **without relying on any third-party engine**. By adopting advanced technologies such as Hybrid row and column storage, GammaDB also possesses the ability to handle HTAP mixed loads.

In preliminary tests, we conducted a performance evaluation on a series of analytical SQL queries. The results demonstrated that, compared with native PostgreSQL, the execution efficiency of some SQL queries was improved by hundreds or even thousands of times.

## NOTE

Currently, GammaDB is recommended for experiments, testing, benchmarking, etc., but **is not recommended for production usage**. If you are interested in GammaDB's benefits in production, please [contact us](mailto:jackey@gammadb.com).

## Setup

1. Before starting working with GammaDB, Modify shared_preload_libraries in postgresql.conf:

```
shared_preload_libraries = 'gammadb'
```

2. Run the following SQL query to create extensions:

```
CREATE EXTENSION gammadb;
```

3. Create columnar tables on GammaDB engine:

```
CREATE TABLE table_name (...) USING gamma;
```

## Roadmap

- [x] 2025Q1
  - [x] Testing and fix some critical bugs (In-progress). 
  - [x] Support spill for HashAgg(20250201 completed).
  - [ ] GammaDB optimizes performance(WIP).
  - [ ] Test the index on the gamma table(WIP).

- [ ] 2025Q2
  - [ ] Support Vectorized Hash Join.

- [ ] 2025Q3
  - [ ] Support S3 storage.

- [ ] 2026Q1-Q4
  - [ ] Support some AI capabilities.
  - [ ] Support some query rewrite optimizations.


## Support

If you're missing a feature or have found a bug, please open a
[GitHub Issue](https://github.com/gammadb/gammadb/issues/new/choose).

To get community support, you can:

- Ask for help on our [GitHub Discussions](https://github.com/gammadb/gammadb/discussions)

If you need commercial support, please [contact the GammaDB team](mailto:jackey@gammadb.com).

## Contributing

We welcome community contributions, big or small, and are here to guide you along
the way. To get started contributing, check our [first timer issues](https://github.com/gammadb/gammadb/labels/good%20first%20issue). 
Once you contribute, ping us in Slack and we'll send you some GammaDB swag!

For more information on how to contribute, please see our
[Contributing Guide](/CONTRIBUTING.md).

This project is released with a [Contributor Code of Conduct](/CODE_OF_CONDUCT.md).
By participating in this project, you agree to follow its terms.

Thank you for helping us make GammaDB better for everyone :heart:.

## License

GammaDB is licensed under the [GNU Affero General Public License v3.0](LICENSE) and as commercial software. For commercial licensing, please contact us at [jackey@gammadb.com](mailto:jackey@gammadb.com).
