# GTFS Diff schema

gtfs-diff-v2-rc1.json is the unmodified MobilityData
[GTFS Diff v2 release-candidate schema](https://github.com/MobilityData/gtfs-diff/blob/d6450a1f5a3dd9478b2ee56b8de9e40d47636dc2/spec/v2/json_schema/v2-rc1.json),
retrieved on 7 September 2026. The formal schema governs field names and types
where the prose specification differs (notably summary counts and column entries).

The diff script runs without third-party packages. Tests optionally use
jsonschema to validate output against this pinned schema.
