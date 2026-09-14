# GTFS changes

Generated: 2026-09-14T10:51:33+00:00

**Comparison:** `ee945dc0c21b:gtfs/bmtc.zip` → `gtfs/bmtc.zip`.
**Feed version:** 20260907 → 20260914. **Validity:** 2026-09-07–2027-09-07 → 2026-09-14–2027-09-14.

## File changes

| File | Before | After | Net | Added | Deleted | Modified |
|---|---:|---:|---:|---:|---:|---:|
| `agency.txt` (unchanged) | 1 | 1 | 0 | 0 | 0 | 0 |
| `attributions.txt` (unchanged) | 2 | 2 | 0 | 0 | 0 | 0 |
| `calendar.txt` | 1 | 49 | +48 | 49 | 1 | 0 |
| `fare_attributes.txt` (deleted) | 110 | 0 | -110 | — | — | — |
| `fare_rules.txt` (deleted) | 170,161 | 0 | -170,161 | — | — | — |
| `feed_info.txt` | 1 | 1 | 0 | 0 | 0 | 1 |
| `routes.txt` | 4,434 | 4,638 | +204 | 4,638 | 4,434 | 0 |
| `shapes.txt` | 2,492,304 | 2,579,006 | +86,702 | 2,579,006 | 2,492,304 | 0 |
| `stop_times.txt` | 1,540,686 | 2,716,638 | +1,175,952 | 2,716,638 | 1,540,686 | 0 |
| `stops.txt` | 9,960 | 9,575 | -385 | 9,575 | 9,960 | 0 |
| `translations.txt` | 9,503 | 9,554 | +51 | 9,554 | 9,503 | 0 |
| `trips.txt` | 57,836 | 84,858 | +27,022 | 84,858 | 57,836 | 0 |

**Total:** 9,519,050 row/column/file changes; 0 files added, 2 deleted, 8 modified, 0 not compared.

## Route service changes

- **Added route names (735):** 107-C D14G-CMS, 108-B, 111-C KBS-VCH-KVB, 114-C STP-SBS-BCGHS, 13 MDP-BSK, 13 SJHS-SBS, 13-B SGH-PNNR, 13-C SGH-ISROL, 13-D, 132, 139-MRS-SBS, 140-E KMT-EJP-KRM; +723 more.
- **Removed route names (531):** 223-L, 226-E KBS-CGD, 234-E SBS-KMT-UST, 234-EC, 238-V KBS-DPN, 240-J DML-CML-KMT, 25-BA, 253-E, 255-B NLM-BELT, 258 SNBS-NMG, 258-C JTMK-GGPFLY-SNBS, 258-MK; +519 more.
- **ID reassignment:** 3,903 retained route names have different route IDs.

Largest changes in scheduled trip records, grouped by route name; directions and duplicate names combined. Renames are not inferred.

| Route | Before | After | Net |
|---|---:|---:|---:|
| 500-D | 261 | 843 | +582 |
| KBS-3A | 269 | 667 | +398 |
| 600-F | 295 | 686 | +391 |
| 226-N | 310 | 693 | +383 |
| V-500D | 196 | 537 | +341 |
| 410-FA | 316 | 639 | +323 |
| 375-D | 398 | 712 | +314 |
| 290-E | 270 | 575 | +305 |

## Notable field changes

No field changes in stops, routes, fare attributes, or translations.
- **stop_times.txt:** columns deleted: stop_headsign.
- **stops.txt:** columns deleted: parent_station, stop_desc, location_type, platform_code.

## Reproduce and interpret

```sh
python3 diff/diff.py
```

[GTFS Diff 2.0.0-rc1](https://github.com/MobilityData/gtfs-diff/blob/main/spec/v2/specification.md) JSON: `diff.json`. Full counts; at most 50 row examples per file across all actions. Rows match declared keys; keyless relations use complete rows. CSV row/column ordering and ZIP metadata are ignored. Column-only changes count once, without inflating modified-row totals. Whole-file additions/deletions count once; their row totals appear in Before/After. Duplicate or missing keys are reported as not compared. Values are compared as strings.

Snapshots are copied before comparison; downloaded_at records local snapshot capture time, not an upstream download time.

- Base SHA-256: `b5fe4a8e05495f719e4c8f16452dc6c4820f342b5414c3fb51e19687b5ea3333`
- New SHA-256: `e34b09cc411977793c457bb9cfc69352869220b81e73b92f24ab4aee6e3b6fe3`
