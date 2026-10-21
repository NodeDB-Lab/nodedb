# `nodedb-crs`

> Coordinate reference systems for NodeDB: an EPSG registry, user-defined CRSs, PROJ-string, WKT2 and PROJJSON text, map projections and datum shifts.

Pure Rust with no I/O and no native dependency, so it builds for `wasm32-unknown-unknown`. NodeDB Origin and Lite resolve and transform every SRID through it.

## What it does

- `CrsContext` resolves an SRID to a `CrsDef`: user-defined CRSs first, then the built-in table.
- `Transformer` maps coordinates between two definitions through ECEF. `apply` runs each time-dependent datum shift at its parameter reference epoch. `apply_at` takes a `CoordinateEpoch`, a decimal year in 1900..=2200.
- `parse_proj_string`, `parse_wkt2` and `parse_projjson` read a definition. `CrsDef::to_proj_string`, `to_wkt2` and `to_projjson` write one back, and a round trip reads back the same definition.
- `CustomCrsSet` holds user-defined CRSs (`CREATE SPATIAL_REF_SYS`). A dropped code stays a tombstone and is never reused.

The SRID type and the `SridResolver` trait live in `nodedb-srid`, which carries no table. A crate that only stores SRIDs depends on `nodedb-srid` and never links the EPSG table.

## Supported CRSs

| Range | Authority | Contents |
|---|---|---|
| 1 to 899 999 | EPSG | 5 916 geographic and projected CRSs from EPSG v12.029 |
| 12287, 25314 to 25322 | EPSG | GDM2020 and its nine state Cassini grids, from EPSG change 2026.081 (`data/epsg_supplement.tsv`) |
| 910 000 to 989 999 | user | `CREATE SPATIAL_REF_SYS` definitions |
| 990 001 | NodeDB | MODIS sinusoidal grid (SR-ORG:6842) |
| 990 002 | NodeDB | Himawari geostationary view, 140.7° E |

Projection methods: Transverse Mercator and UTM, Hotine Oblique Mercator (variants A and B), Cassini-Soldner (and hyperbolic), Mercator (variants A and B), Pseudo-Mercator, Lambert Cylindrical Equal Area, Sinusoidal, Geostationary Satellite, Lambert Conic Conformal (1SP and 2SP), Polar and oblique Stereographic, UPS, Albers Equal Area, Lambert Azimuthal Equal Area, Krovak (and modified), Swiss Oblique Mercator, American Polyconic, Equidistant Cylindrical, Equal Earth, Azimuthal Equidistant (and Guam), New Zealand Map Grid, and Laborde Oblique Mercator.

Datum shifts: 3-, 7- and 15-parameter (time-dependent) Helmert, position-vector and coordinate-frame conventions, IOGP Guidance Note 7-2 §4.2.3 and §4.2.5 (EPSG methods 9603, 9606, 9607, 1031, 1032, 1033, 1035, 1037, 1038, and 1053 to 1058). WKT2 and PROJJSON read and write the rates and the parameter reference epoch. A PROJ string has no rates: `to_proj_string` refuses a time-dependent shift with `ProjExportError::TimeDependentShift`, as PROJ does. Linear units: every PROJ `+units` name and any `+to_meter` factor.

The table holds every non-deprecated EPSG geographic 2D and projected CRS, except the few the generator skips with a stated reason: methods with no implementation here, such as Bonne (South Orientated), Colombia Urban and the Tunisia Mining Grid. A datum EPSG relates to WGS 84 by a grid carries its best Helmert transformation, or no shift.

## GDM2020

GDM2020 (Geocentric Datum of Malaysia 2020, EPSG datum 1471) is in the EPSG registry from change 2026.081, revision 2026-10-05. No released PROJ or pyproj bundles it: PROJ 9.9.0 carries EPSG v13.102. `data/epsg_supplement.tsv` holds its rows with their EPSG codes:

| Code | CRS |
|---|---|
| 12287 | GDM2020, geographic 2D, GRS 1980 |
| 25314 | GDM2020 / Johor Grid |
| 25315 | GDM2020 / Sembilan and Melaka Grid |
| 25316 | GDM2020 / Pahang Grid |
| 25317 | GDM2020 / Selangor Grid |
| 25318 | GDM2020 / Terengganu Grid |
| 25319 | GDM2020 / Pinang Grid |
| 25320 | GDM2020 / Kedah and Perlis Grid |
| 25321 | GDM2020 / Perak Grid |
| 25322 | GDM2020 / Kelantan Grid |

Sources:

- EPSG Geodetic Parameter Registry API, `https://apps.epsg.org/api/v1/`: datum 1471, CRS 12287, conversions 12314 to 12322 (Cassini-Soldner, origins in sexagesimal DMS, false easting and northing 0 m), projected CRSs 25314 to 25322, transformation 12291 (GDM2020 to WGS 84, null geocentric translations, accuracy 1 m).
- JUPEM, Technical Guide to the Coordinate Conversion, Datum Transformation and Map Projection, circular KPUP 3/2021: Table 7 gives the same GDM2020 state origins. Test example 4.6 (i) checks EPSG:25320. Its printed easting is 1.000 m off the Cassini-Soldner formula, and its northing matches to the millimetre.

GDM2000 to GDM2020 needs JUPEM's velocity and post-seismic deformation grids, which JUPEM does not publish openly, so the crate has no shift between them. GDM2020 Peninsular RSO and East Malaysia BRSO have no EPSG code, and JUPEM withholds their parameters (KPUP 3/2021 Figure 14), so the crate does not define them.

## Accuracy against PROJ

`tests/proj_reference.rs` transforms the centre and two inset corners of each CRS's area of use, forward and inverse, and compares with PROJ 9.8.1. A point agrees within 1 mm projected and 1e-9 degrees geographic, unless its fixture row carries its own tolerance in metres.

The crate applies no grid shift (NADCON, NTv2, geoid models). Where PROJ picks a grid transformation between two datums, the result here differs by that grid's correction, up to several metres. The view `nodedb.spatial_ref_sys` shows each built-in's published Helmert accuracy in `datum_shift_accuracy_m`.

## Regenerating the EPSG table

`data/epsg.tsv` is generated from a pinned pyproj (3.8.0, bundling PROJ 9.8.1 and EPSG v12.029). The script refuses any other version and exits non-zero on a method the Rust parser does not implement. It also writes `data/epsg_supplement.tsv` from its `SUPPLEMENT` definitions, and stops when the pinned EPSG dataset already holds one of those codes.

Create the virtual environment under the workspace build directory, never under `/tmp`:

```bash
python3 -m venv target-cluster/epsg-venv
target-cluster/epsg-venv/bin/pip install pyproj==3.8.0
target-cluster/epsg-venv/bin/python scripts/gen_epsg.py              # write data/epsg.tsv
target-cluster/epsg-venv/bin/python scripts/gen_epsg.py --fixtures   # also write tests/fixtures/*.tsv
target-cluster/epsg-venv/bin/python scripts/gen_epsg.py --check      # compare with the committed files
```

## Status

Pre-1.0. APIs can change between minor versions until 1.0. See the workspace [`README.md`](../README.md) for the full project overview and the GitHub [release notes](https://github.com/NodeDB-Lab/nodedb/releases) for per-version changes.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE-APACHE`](../LICENSE.APACHE-2.0)).
