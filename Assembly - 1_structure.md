# File Structure Report: Assembly - 1.xlsx

**Date:** 2026-03-08 17:02:50

## Overview

- **Entities:** 2 — LTD, INC
- **Consolidated:** Yes
- **NIS columns:** Yes
- **AJE columns:** Yes
- **COA columns:** B

> This file contains a combined balance sheet and profit/loss (BS+PL) on one main sheet ('FS') with data in NIS and USD. Two entities (LTD, INC) are included, with per-entity values, USD conversions, and a consolidated column (via formula summing USD entity columns). Adjusting journal entries and budget columns are also present, with comparative/pro-forma values for the prior year. No COA numeric codes are used.

## Sheet: FS (BS+PL)

- **Unit:** NIS
- **Data rows:** 11 - 199
- **Header rows:** [1, 10]

### Key Columns (COA, Entities, AJE, Consolidated)

| Column | Sheet | Role | Entity | Currency | Row Start | Row End | Header | Formula |
|--------|-------|------|--------|----------|-----------|---------|--------|---------|
| B | FS | coa_name |  |  | 11 | 199 | Account description |  |
| C | FS | entity_value | LTD | NIS | 11 | 199 | LTD | =SUMIF(GL_LTD!$J:$J,A[row],GL_LTD!$I:$I) |
| E | FS | entity_value | LTD | USD | 11 | 199 | $ | =SUMIF(GL_LTD!$J:$J,A[row],GL_LTD!$G:$G) |
| I | FS | entity_value | INC | USD | 11 | 199 | INC | =SUMIF(GL_INC!G:G,A[row],GL_INC!F:F) |
| K | FS | aje | AJE | USD | 11 | 199 | AJE |  |
| M | FS | consolidated | Consolidated | USD | 11 | 199 | Consolidated | =SUM(E[row]+I[row]) |

### All Columns

| Column | Index | Sheet | Role | Entity | Currency | Period | Row Start | Row End | Header | Formula |
|--------|-------|-------|------|--------|----------|--------|-----------|---------|--------|---------|
| B | 2 | FS | coa_name |  |  |  | 11 | 199 | Account description | =SUM(E[row]+I[row]) |
| C | 3 | FS | entity_value | LTD | NIS | 2022 | 11 | 199 | LTD | =SUM(E[row]+I[row]) |
| E | 5 | FS | entity_value | LTD | USD | 2022 | 11 | 199 | $ | =SUM(E[row]+I[row]) |
| G | 7 | FS | other |  |  |  | 11 | 199 | FX rate | =SUM(E[row]+I[row]) |
| I | 9 | FS | entity_value | INC | USD | 2022 | 11 | 199 | INC | =SUM(E[row]+I[row]) |
| K | 11 | FS | aje | AJE | USD | 2022 | 11 | 199 | AJE | =SUM(E[row]+I[row]) |
| M | 13 | FS | consolidated | Consolidated | USD | 2022 | 11 | 199 | Consolidated | =SUM(E[row]+I[row]) |
| O | 15 | FS | prior_period |  | USD | 2021 | 11 | 199 | 31/12/2021 | =SUM(E[row]+I[row]) |
| P | 16 | FS | budget |  |  | Budget | 82 | 185 | Budget | =SUM(E[row]+I[row]) |

## Notes

- Single sheet covers both BS and PL.
- Entity data mapped for 2022 only; prior years shown as single column.
- No chart of accounts (COA) numeric codes, only description names in column B.
- All core financial columns exist on FS main sheet; GL_LTD/GL_INC are single-entity source sheets, their columns not listed in mapping.
- Currency: Primary in NIS, with USD conversions for each entity and consolidated.
- AJE adjusting column (K) allows additional entity-level adjustments.
- FX rate column (G) is informational only.
- Budget data (column P) not always present for all rows.
