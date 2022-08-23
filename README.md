## package chutils
[![Go Report Card](https://goreportcard.com/badge/github.com/invertedv/chutils)](https://goreportcard.com/report/github.com/invertedv/chutils)
[![godoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/mod/github.com/invertedv/chutils?tab=overview)

ClickHouse is an excellent database for analysis applications.  It's exceptionally fast and has super useful
data structures (such as Nested Arrays).

Getting text files into most databases can be a hassle. There is often random garbage in fields.
Column order varies.  The destination table has to exist.  For materializing output of a query,
ClickHouse supports CREATE MATERIALIZED VIEW or you can also create a table from the output of a query
as CREATE TABLE from SELECT Query.  Even in these cases, there may be other changes you may wish to make.
For instance, nesting arrays or data validation.

The chutils package facilitates these types of functions. The principal use cases are:

1. file --> ClickHouse
2. ClickHouse --> file
3. ClickHouse --> ClickHouse


Features include:

**Reading Data**
- Point and shoot -- chutils will impute field types from text files.
- Field types can also be user-specified.  The results of the imputation can be augmented or overridden.
- Import files without headers.
- Import fixed-width (flat) files.
- Range checking for int/float fields
- Levels checking for String/FixedString fields
- Complex adjustments to a field that are more easily implemented in Go than SQL
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

Why is use case 3 helpful?
- Automatic generation of the CREATE TABLE statement
- Data cleaning
- Renaming fields
- Adding fields that may be complex functions of the Input and/or use data from other Go variables.
