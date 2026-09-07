# GTFS changes

Generated: 2026-09-07T07:01:48+00:00

**Comparison:** `9b10e7bacbd5:gtfs/bmtc.zip` → `gtfs/bmtc.zip`.
**Feed version:** 20260712 → 20260907. **Validity:** 2026-07-12–2027-07-12 → 2026-09-07–2027-09-07.

## File changes

| File | Before | After | Net | Added | Deleted | Modified |
|---|---:|---:|---:|---:|---:|---:|
| `agency.txt` (unchanged) | 1 | 1 | 0 | 0 | 0 | 0 |
| `attributions.txt` (unchanged) | 2 | 2 | 0 | 0 | 0 | 0 |
| `calendar.txt` | 1 | 1 | 0 | 0 | 0 | 1 |
| `fare_attributes.txt` | 120 | 110 | -10 | 0 | 10 | 0 |
| `fare_rules.txt` | 261,613 | 170,161 | -91,452 | 151,900 | 243,352 | 0 |
| `feed_info.txt` | 1 | 1 | 0 | 0 | 0 | 1 |
| `routes.txt` | 4,381 | 4,434 | +53 | 241 | 188 | 14 |
| `shapes.txt` | 2,447,719 | 2,492,304 | +44,585 | 150,431 | 105,846 | 463,894 |
| `stop_times.txt` | 1,519,120 | 1,540,686 | +21,566 | 467,529 | 445,963 | 1,069,356 |
| `stops.txt` | 9,887 | 9,960 | +73 | 205 | 132 | 5,270 |
| `translations.txt` | 9,437 | 9,503 | +66 | 172 | 106 | 38 |
| `trips.txt` | 56,855 | 57,836 | +981 | 981 | 0 | 53,403 |

**Total:** 3,159,033 row/column/file changes; 0 files added, 0 deleted, 10 modified, 0 not compared.

**Trip-ID caveat:** 53,235/56,855 shared trip IDs now refer to a different route/direction. Trip and stop-time modifications are literal ID-based changes, not counts of changed or cancelled services. Shape-point changes also include geometry resampling and sequence shifts.

## Route service changes

- **Added route names (238):** 112 SBS-HIRT, 12 KBS-BSK-KDHC, 13 WG10TH-MYH, 143 NRG-SNBS, 15-E KBS-KML, 180-A MRBS-HIRT, 2, 201 D6-BSK, 201-G  BSK-HAL ARDC, 201-G HAL ARDC-BSK-KGR, 210-N KBS-UTH-GPTA, 211-C; +226 more.
- **Removed route names (185):** 13 MDP-BSK, 13 SJHS-SBS, 139-MRS-SBS, 144-E SNBS-SPHS, 161-D SBS-BTMS, 176-C STP-IIHS, 201-G BSK-BNM, 211-AD, 215-S SJBS-JSD, 223-A HAL-DBT, 225-K KBS-BHELLO, 226-Y; +173 more.
- **ID reassignment:** 12 retained route names have different route IDs.

Largest changes in scheduled trip records, grouped by route name; directions and duplicate names combined. Renames are not inferred.

| Route | Before | After | Net |
|---|---:|---:|---:|
| 502-HC | 0 | 173 | +173 |
| D40-D40G | 9 | 137 | +128 |
| D40-DSP | 127 | 4 | -123 |
| D20-BSK | 166 | 81 | -85 |
| VV-EXP 298MN | 0 | 85 | +85 |
| O EXP V-317CH | 0 | 81 | +81 |
| O EXP-317CH | 81 | 0 | -81 |
| 258-C D40G-NMG | 4 | 78 | +74 |

## Notable field changes

- **stops.txt:** zone_id 5,164; stop_lon 202; stop_lat 201; stop_desc 30 modified rows.
- **routes.txt:** route_short_name 9; route_long_name 5 modified rows.
- **translations.txt:** translation 38 modified rows.

## Reproduce and interpret

```sh
python3 diff/diff.py
```

[GTFS Diff 2.0.0-rc1](https://github.com/MobilityData/gtfs-diff/blob/main/spec/v2/specification.md) JSON: `diff.json`. Full counts; at most 50 row examples per file across all actions. Rows match declared keys; keyless relations use complete rows. CSV row/column ordering and ZIP metadata are ignored. Column-only changes count once, without inflating modified-row totals. Whole-file additions/deletions count once; their row totals appear in Before/After. Duplicate or missing keys are reported as not compared. Values are compared as strings.

Snapshots are copied before comparison; downloaded_at records local snapshot capture time, not an upstream download time.

- Base SHA-256: `2308f8248ea954b75b3660cda7b6d85ecf80831179968baec8ae3db5b41b0b4e`
- New SHA-256: `b5fe4a8e05495f719e4c8f16452dc6c4820f342b5414c3fb51e19687b5ea3333`
