# GTFS changes

Generated: 2026-11-04T17:05:48+00:00

**Comparison:** `ee945dc0c21b:gtfs/bmtc.zip` → `gtfs/bmtc.zip`.
**Feed version:** 20260907 → 20261104. **Validity:** 2026-09-07–2027-09-07 → 2026-11-04–2027-11-04.

## File changes

| File | Before | After | Net | Added | Deleted | Modified |
|---|---:|---:|---:|---:|---:|---:|
| `agency.txt` | 1 | 1 | 0 | 1 | 1 | 0 |
| `attributions.txt` | 2 | 2 | 0 | 0 | 0 | 2 |
| `calendar.txt` | 1 | 52 | +51 | 52 | 1 | 0 |
| `fare_attributes.txt` | 110 | 86 | -24 | 41 | 65 | 45 |
| `fare_rules.txt` | 170,161 | 6,608 | -163,553 | 6,608 | 170,161 | 0 |
| `feed_info.txt` | 1 | 1 | 0 | 0 | 0 | 1 |
| `routes.txt` | 4,434 | 5,000 | +566 | 5,000 | 4,434 | 0 |
| `shapes.txt` | 2,492,304 | 4,236,328 | +1,744,024 | 4,236,328 | 2,492,304 | 0 |
| `stop_times.txt` | 1,540,686 | 2,237,974 | +697,288 | 2,237,974 | 1,540,686 | 0 |
| `stops.txt` | 9,960 | 10,128 | +168 | 10,128 | 9,960 | 0 |
| `translations.txt` | 9,503 | 9,910 | +407 | 9,910 | 9,503 | 0 |
| `trips.txt` | 57,836 | 69,598 | +11,762 | 69,598 | 57,836 | 0 |

**Total:** 10,860,651 row/column/file changes; 0 files added, 0 deleted, 12 modified, 0 not compared.

## Route service changes

- **Added route names (845):** 107-C D14G-CMS, 108-B, 111-C KBS-VCH-KVB, 114-C STP-SBS-BCGHS, 13 MDP-BSK, 13 SJHS-SBS, 13-B SGH-PNNR, 13-C SGH-ISROL, 13-D, 132, 139-MRS-SBS, 140-E KMT-EJP-KRM; +833 more.
- **Removed route names (279):** 107-C D14G-SBS, 15-A PPLO-UTH-KMT, 2-E  JPN6-VSD, 201-G  BSK-HAL ARDC, 210-P CSBS-NGH, 215-E, 215-HA D34G-LBN, 225-CA KBS-BEML5-CNS, 226-E KBS-CGD, 234-E SBS-KMT-UST, 234-EC, 238-V KBS-DPN; +267 more.
- **ID reassignment:** 4,155 retained route names have different route IDs.

Largest changes in scheduled trip records, grouped by route name; directions and duplicate names combined. Renames are not inferred.

| Route | Before | After | Net |
|---|---:|---:|---:|
| D13-KMK | 188 | 0 | -188 |
| D2-SNBS | 172 | 0 | -172 |
| V-335E | 122 | 294 | +172 |
| D33-PPLO | 171 | 0 | -171 |
| D7-KBS | 170 | 0 | -170 |
| D28-HBLB | 166 | 0 | -166 |
| D29-KRPGH | 165 | 0 | -165 |
| D38-D38G | 160 | 0 | -160 |

## Notable field changes

- **fare_attributes.txt:** agency_id 45 modified rows.
- **fare_rules.txt:** columns added: contains_id.
- **routes.txt:** columns added: route_desc, route_url.
- **stop_times.txt:** columns added: shape_dist_traveled, timepoint.
- **stop_times.txt:** columns deleted: stop_headsign.
- **stops.txt:** columns added: stop_code, stop_url, wheelchair_boarding.
- **trips.txt:** columns added: trip_short_name, wheelchair_accessible, bikes_allowed.

## Reproduce and interpret

```sh
python3 diff/diff.py --base-ref origin/main
```

[GTFS Diff 2.0.0-rc1](https://github.com/MobilityData/gtfs-diff/blob/main/spec/v2/specification.md) JSON: `diff.json`. Full counts; at most 50 row examples per file across all actions. Rows match declared keys; keyless relations use complete rows. CSV row/column ordering and ZIP metadata are ignored. Column-only changes count once, without inflating modified-row totals. Whole-file additions/deletions count once; their row totals appear in Before/After. Duplicate or missing keys are reported as not compared. Values are compared as strings.

Snapshots are copied before comparison; downloaded_at records local snapshot capture time, not an upstream download time.

- Base SHA-256: `b5fe4a8e05495f719e4c8f16452dc6c4820f342b5414c3fb51e19687b5ea3333`
- New SHA-256: `e41e4d8a153f3ad3ef73a9f84ef3d59fd4fd0abf26d690837c5e8132098d83ba`
