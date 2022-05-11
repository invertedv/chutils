### package chutils

ClickHouse is an excellent database for analysis applications.  It's exceptionally fast and has super useful
data structures (Nested!).  It's my go-to solution.

Getting text files into most any database can be a hassle. There is often random garbage in fields.
Column order varies.  The destination table has to exist.  For materializing output of a query,
ClickHouse supports CREATE MATERIALIZED VIEW. This view updates any time the underlying table(s) update.
There may be times when that's not what you want.  Then you're on your own generating CREATE TABLE statements.

The chutils package facilitates these types of functions. The principal use cases are:

- Importing text files to ClickHouse
- Exporting from ClickHouse to text files
- Running queries that create new tables

Features include:

**Reading Data**
- Point and shoot -- chutils will impute field types from text files.
- Field types can also be user-specified.  The results of the imputation can be augmented or overridden.
- Import files without headers.
- Import fixed-width (flat) files.
- Range checking for int/float fields
- Levels checking for String/FixedString fields
- More complex adjustments to field that are more easily implemented in Go than SQL
- Creation of new fields
- Reading from either text files or SQL
- ClickHouse CREATE TABLE generation
<br>

**Writing Data**
- Creation of text files
- Creation of ClickHouse tables via three routes:
    - from textfiles using clickhouse-client (package file)
    - from VALUES statements built with sql.Writer
    - from SQL directly using sql.Reader


