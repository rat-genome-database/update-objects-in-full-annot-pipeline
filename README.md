# update-objects-in-full-annot-pipeline

Synchronizes object symbols and names in the RGD `FULL_ANNOT` table with their authoritative source tables.
Over time, gene symbols, strain names, QTL names, and other object metadata can change in the source tables;
this pipeline detects those mismatches and updates `FULL_ANNOT` to stay in sync.

## Supported object types

| Object Type | RGD_OBJECT_KEY | Source Table | Symbol Column | Name Column |
|---|---|---|---|---|
| Genes | 1 | `GENES` | `GENE_SYMBOL` | `FULL_NAME` |
| Strains | 5 | `STRAINS` | `STRAIN_SYMBOL` | `FULL_NAME` |
| QTLs | 6 | `QTLS` | `QTL_SYMBOL` | `QTL_NAME` |
| ClinVar Variants | 7 | `GENOMIC_ELEMENTS` | `SYMBOL` | `NAME` |
| Cell Lines | 11 | `GENOMIC_ELEMENTS` | `SYMBOL` | `NAME` |

## How it works

For each object type the pipeline:

1. Joins `FULL_ANNOT` with the source table on `ANNOTATED_OBJECT_RGD_ID = RGD_ID`, filtering by the appropriate `RGD_OBJECT_KEY`.
2. Selects rows where the symbol or name differs (using `NVL` to handle NULLs).
3. Updates `OBJECT_SYMBOL`, `OBJECT_NAME`, `LAST_MODIFIED_DATE`, and `LAST_MODIFIED_BY` in `FULL_ANNOT` for each mismatched row.
4. Logs per-type counts of symbol changes, name changes, and distinct objects affected.

## Configuration

- **Spring beans** &mdash; `properties/AppConfigure.xml` injects the `version` string and `lastModifiedBy` user ID (default `170`).
- **Database connection** &mdash; configured externally via `default_db2.xml` (referenced in `_run.sh`).
- **Logging** &mdash; `properties/log4j2.xml` writes three log files under `logs/`:
  - `detail.log` &mdash; DEBUG-level, monthly rollover.
  - `status.log` &mdash; INFO-level, monthly rollover.
  - `summary.log` &mdash; INFO-level, overwritten each run (used for email notifications).
