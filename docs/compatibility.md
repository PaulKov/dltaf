# Compatibility and Migration

`dltaf` keeps older SQL manifests working, but the public recommendation is to move new authoring to canonical `sqldb`.

## Legacy SQL kinds still accepted

- `sql_database`
- `oracle_custom_sql`
- `oracle`

They are compatibility aliases, not the preferred authoring style.

## Migration target

### Legacy catalog style

```yaml
source:
  kind: sql_database
  schema: public
  tables:
    - orders
```

Preferred canonical form:

```yaml
source:
  kind: sqldb
  dialect: generic
  mode: catalog
  catalog:
    schema: public
    tables:
      - orders
```

### Legacy Oracle query style

```yaml
source:
  kind: oracle_custom_sql
  queries:
    - name: q1
      sql_file: sql/q1.sql
```

Preferred canonical form:

```yaml
source:
  kind: sqldb
  dialect: oracle
  mode: query
  query:
    queries:
      - name: q1
        sql_file: sql/q1.sql
```

## Tooling support

Use `manifest doctor` to generate or review canonical forms:

```bash
dltaf manifest doctor --template-kind sqldb_catalog --pipeline-name dlt__postgres__to__clickhouse__raw
dltaf manifest doctor --template-kind sqldb_query --pipeline-name dlt__oracle__to__clickhouse__raw
```

The doctor also flags deprecated SQL source kinds and suggests canonical replacements.

## Why keep aliases at all

Because migration is a real operational problem.

The goal is:

- do not break old manifests
- help new manifests start clean
- let teams migrate at their own pace
