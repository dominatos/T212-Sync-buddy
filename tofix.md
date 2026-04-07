# Known Issues / Problems to Fix

*Items are removed from this list only by the Project Manager after confirmed resolution.*

## 1. Verification false-positives due to upstream symbol remapping --FIXED in first run test. now test for second run

**Status:** Fix in progress  
**Affected accounts:** SV (4 rows), YU (2 rows)  
**Affected ticker:** `VEVE` (`IE00BKX55T58`) → remapped to `VEVEL.XC` by upstream converter  

**Description:**  
The `dickwolff/export-to-ghostfolio` converter sometimes remaps Trading212 ticker symbols when resolving ISINs against Yahoo Finance. For example, T212 uses `VEVE` for the Vanguard FTSE Developed World ETF (ISIN `IE00BKX55T58`), but the converter resolves it to `VEVEL.XC`.

The verification step in `run-all.sh` uses `date_symbol_quantity` as the matching key between CSV rows and JSON activities. When the symbol is remapped, legitimate trades are flagged as discrepancies and CSVs get quarantined — a false positive.

**Impact:**  
- CSVs are quarantined despite all activities being successfully imported into Ghostfolio
- State is not persisted, causing full re-import on the next run
- The data that WAS imported (662 activities for SV, 459 for YU) is correct

**Fix:**  
Change the verification key from `date_symbol_quantity` to `date_quantity_unitPrice`. This eliminates the symbol dependency while maintaining collision resistance (same date + same quantity + same unit price for different stocks is practically impossible with fractional shares).

**Quarantined files to re-process after fix:**  
- `sv-2026-04-07-202542.csv`
- `sv-2026-04-07-203840.csv`
- `yu-2026-04-07-202315.csv`
- `yu-2026-04-07-203614.csv`
