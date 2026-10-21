#!/usr/bin/env python3
"""Generate the nodedb-crs EPSG table and PROJ reference fixtures from a pinned pyproj.

Usage:
    python scripts/gen_epsg.py              write nodedb-crs/data/epsg.tsv and epsg_supplement.tsv
    python scripts/gen_epsg.py --fixtures   also write nodedb-crs/tests/fixtures/*.tsv
    python scripts/gen_epsg.py --check      regenerate in memory and compare with the committed files

The script needs pyproj == PYPROJ_VERSION, which bundles PROJ == PROJ_VERSION and
EPSG database EPSG_VERSION. Any other version stops the script.

epsg_supplement.tsv holds EPSG CRSs published after EPSG_VERSION, written from SUPPLEMENT.
The script stops when the pinned database already holds a SUPPLEMENT code.
"""

from __future__ import annotations

import os

# Network grids change PROJ's operation ranking, so the script never fetches them.
os.environ["PROJ_NETWORK"] = "OFF"

import argparse
import math
import sys
import warnings
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import pyproj
from pyproj import CRS, Transformer
from pyproj.aoi import AreaOfInterest
from pyproj.database import get_database_metadata, query_crs_info
from pyproj.enums import PJType
from pyproj.transformer import TransformerGroup

warnings.simplefilter("ignore")
pyproj.network.set_network_enabled(False)

PYPROJ_VERSION = "3.8.0"
PROJ_VERSION = "9.8.1"
EPSG_VERSION = "v12.029"

ROOT = Path(__file__).resolve().parents[1]
TSV_PATH = ROOT / "nodedb-crs" / "data" / "epsg.tsv"
FIXTURE_PATH = ROOT / "nodedb-crs" / "tests" / "fixtures" / "proj_reference.tsv"
MALAYSIA_PATH = ROOT / "nodedb-crs" / "tests" / "fixtures" / "proj_reference_malaysia.tsv"
SUPPLEMENT_PATH = ROOT / "nodedb-crs" / "data" / "epsg_supplement.tsv"

# The Rust parser implements exactly these +proj values.
ALLOWED_PROJ = frozenset(
    "longlat utm tmerc omerc cass merc cea sinu geos lcc stere sterea ups aea laea "
    "krovak mod_krovak somerc poly eqc eqearth aeqd nzmg labrd webmerc".split()
)

# The Rust parser reads exactly these keys.
ALLOWED_KEYS = frozenset(
    "+proj +zone +south +lat_0 +lon_0 +lat_1 +lat_2 +lat_ts +lonc +alpha +gamma +k +k_0 "
    "+x_0 +y_0 +h +ellps +a +b +rf +f +R +datum +towgs84 +pm +units +to_meter +no_uoff "
    "+no_rot +axis +no_defs +type +azi +sweep +czech +guam +hyperbolic".split()
)

ELLIPSOID_KEYS = ("+ellps", "+a", "+b", "+rf", "+f", "+R")

# EPSG operation method code -> the +proj values that implement it.
METHOD_PROJ: dict[str, frozenset[str]] = {
    "9801": frozenset({"lcc"}),  # Lambert Conic Conformal (1SP)
    "9802": frozenset({"lcc"}),  # Lambert Conic Conformal (2SP)
    "9803": frozenset({"lcc"}),  # Lambert Conic Conformal (2SP Belgium)
    "9804": frozenset({"merc"}),  # Mercator (variant A)
    "9805": frozenset({"merc"}),  # Mercator (variant B)
    "9806": frozenset({"cass"}),  # Cassini-Soldner
    "9807": frozenset({"tmerc", "utm"}),  # Transverse Mercator
    "9808": frozenset({"tmerc"}),  # Transverse Mercator (South Orientated)
    "9809": frozenset({"sterea"}),  # Oblique Stereographic
    "9810": frozenset({"stere"}),  # Polar Stereographic (variant A)
    "9811": frozenset({"nzmg"}),  # New Zealand Map Grid
    "9812": frozenset({"omerc"}),  # Hotine Oblique Mercator (variant A)
    "9813": frozenset({"labrd"}),  # Laborde Oblique Mercator
    "9814": frozenset({"somerc"}),  # Swiss Oblique Mercator
    "9815": frozenset({"omerc", "somerc"}),  # Hotine Oblique Mercator (variant B)
    "9818": frozenset({"poly"}),  # American Polyconic
    "9819": frozenset({"krovak"}),  # Krovak
    "9820": frozenset({"laea"}),  # Lambert Azimuthal Equal Area
    "9822": frozenset({"aea"}),  # Albers Equal Area
    "9826": frozenset({"lcc"}),  # Lambert Conic Conformal (West Orientated)
    "9829": frozenset({"stere"}),  # Polar Stereographic (variant B)
    "9830": frozenset({"stere"}),  # Polar Stereographic (variant C)
    "9831": frozenset({"aeqd"}),  # Guam Projection
    "9832": frozenset({"aeqd"}),  # Modified Azimuthal Equidistant
    "9833": frozenset({"cass"}),  # Hyperbolic Cassini-Soldner
    "9834": frozenset({"cea"}),  # Lambert Cylindrical Equal Area (Spherical)
    "9835": frozenset({"cea"}),  # Lambert Cylindrical Equal Area
    "1024": frozenset({"webmerc"}),  # Popular Visualisation Pseudo Mercator
    "1026": frozenset({"merc"}),  # Mercator (Spherical)
    "1027": frozenset({"laea"}),  # Lambert Azimuthal Equal Area (Spherical)
    "1028": frozenset({"eqc"}),  # Equidistant Cylindrical
    "1029": frozenset({"eqc"}),  # Equidistant Cylindrical (Spherical)
    "1041": frozenset({"krovak"}),  # Krovak (North Orientated)
    "1042": frozenset({"mod_krovak"}),  # Krovak Modified
    "1043": frozenset({"mod_krovak"}),  # Krovak Modified (North Orientated)
    "1051": frozenset({"lcc"}),  # Lambert Conic Conformal (2SP Michigan)
    "1078": frozenset({"eqearth"}),  # Equal Earth
    "1102": frozenset({"lcc"}),  # Lambert Conic Conformal (1SP variant B)
    "1125": frozenset({"aeqd"}),  # Azimuthal Equidistant
}

METHOD_PSEUDO_MERCATOR = "1024"
METHOD_LCC_WEST = "9826"
METHOD_POLAR_STEREO_C = "9830"

# Krovak output in PROJ already follows the EPSG southing/westing axes.
NATIVE_AXIS_METHODS = frozenset({"9819", "1041", "1042", "1043"})
AXIS_LETTER = {"east": "e", "west": "w", "north": "n", "south": "s"}

PSEUDO_MERCATOR_PROJ ="+proj=webmerc +datum=WGS84 +units=m +no_defs"
WGS84_PROJ = "+proj=longlat +datum=WGS84 +no_defs"

# PROJ datum names that to_proj4 emits, mapped to their PROJ ellipsoid names.
# WGS84 stays as +datum=WGS84. The others become an explicit ellipsoid plus +towgs84.
DATUM_ELLPS = {"NAD83": "GRS80", "NAD27": "clrk66"}

# EPSG Helmert method codes: translations only, position vector, coordinate frame.
HELMERT_TRANSLATION = frozenset({"9603", "1031", "1035"})
HELMERT_POSITION_VECTOR = frozenset({"9606", "1033", "1037"})
HELMERT_COORDINATE_FRAME = frozenset({"9607", "1032", "1038"})
HELMERT_METHODS = HELMERT_TRANSLATION | HELMERT_POSITION_VECTOR | HELMERT_COORDINATE_FRAME

# Conversion steps that PROJ wraps around a Helmert without changing its math.
NEUTRAL_CONVERSIONS = frozenset({"9843", "9602", "9659"})

# PROJ pipeline steps around a Helmert that change units or axes only.
# A longlat step only names the ellipsoid of the next cart step.
PIPELINE_NEUTRAL = frozenset(
    {"pipeline", "unitconvert", "push", "pop", "cart", "axisswap", "noop", "longlat"}
)
# Keys of a static PROJ helmert step: metres, arc-seconds and ppm.
HELMERT_PIPELINE_KEYS = frozenset({"x", "y", "z", "rx", "ry", "rz", "s", "convention"})

# A PROJ string matches EPSG when every check point agrees within this distance.
PROJECTION_TOLERANCE_M = 1e-3
# Degrees per metre on the equator, for comparing datum shifts in lon/lat.
DEG_PER_M = 1.0 / 111_319.49
DATUM_TOLERANCE_DEG = 1e-3 * DEG_PER_M

# EPSG Guidance Note 7-2 worked example for Polar Stereographic (variant C).
PS_C_EXAMPLE_PARAMS = (-67.0, 140.0, 300000.0, 200000.0)
PS_C_EXAMPLE_ELLIPSOID = "International 1924"
PS_C_EXAMPLE_LONLAT = (140 + 4 / 60 + 17.040 / 3600, -(66 + 36 / 60 + 18.820 / 3600))
PS_C_EXAMPLE_EN = (303169.522, 244055.721)
# The example prints millimetres, so the check allows one millimetre.
PS_C_EXAMPLE_TOLERANCE_M = 1e-3

INSET = 0.05

# Codes the generator skips on purpose, with the reason.
# Every other code outside ALLOWED_PROJ or ALLOWED_KEYS fails --check.
SKIP: dict[int, str] = {
    **{
        code: "Colombia Urban (1052): no +proj in the implemented list, PROJ needs +h_0"
        for code in range(6244, 6276)
    },
    2963: "Bonne (South Orientated) (9828): +proj=bonne is not implemented",
    5017: "Bonne (South Orientated) (9828): +proj=bonne is not implemented",
    9311: "LAEA (Spherical) (1027) on NAD27: +R_A is not in the key list, +R changes the datum shift",
    10622: "Local Orthographic (1130): +proj=ortho is not implemented",
    22300: "Tunisia Mining Grid (9816): PROJ has no implementation",
    22700: "Lambert Conic Near-Conformal (9817): +proj=lcca is not implemented",
    32600: "Transverse Mercator Zoned Grid System (9824): the zone depends on the point",
    32700: "Transverse Mercator Zoned Grid System (9824): the zone depends on the point",
}

# Malaysian codes for the official-EPSG fixture.
# 3377-3385 are the GDM2000 state Cassini grids.
# GDM2020 is not in EPSG v12.029, so PROJ cannot build it from its codes.
# SUPPLEMENT defines it, and proj_reference.tsv covers it.
MALAYSIA_CODES = (
    [4326, 4742, 4245, 4298, 3375, 3376, 3168, 3167, 3380, 29871, 29872, 29873]
    + [32647, 32648, 32649, 32650]
    + list(range(3377, 3386))
    + [3857, 6933]
)

# Lon/lat boxes for Peninsular Malaysia, Sarawak and Sabah.
MALAYSIA_REGIONS = (
    (99.6, 1.25, 104.5, 6.7),
    (109.6, 0.9, 115.6, 5.0),
    (115.4, 4.1, 119.3, 7.3),
)
MALAYSIA_POINTS = 10
# The epsg.tsv strings reproduce a Malaysian row when they agree within these.
MALAYSIA_MATCH_M = 1e-3
MALAYSIA_MATCH_DEG = 1e-9
# Metres per degree on the equator, rounded up. Converts a degree error to metres.
METRES_PER_DEGREE_BOUND = 111_320.0

# Synthetic satellite grids with no EPSG code.
# Himawari uses the CGMS normalized geostationary projection, which is PROJ's default sweep=y.
SYNTHETIC = {
    990001: "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs",
    990002: "+proj=geos +h=35785863 +lon_0=140.7 +ellps=WGS84 +units=m +no_defs",
}
SYNTHETIC_POINTS = {
    990001: [
        (-170.0, -60.0), (-120.0, -30.0), (-60.0, 0.5), (0.0, 0.0), (30.0, 45.0),
        (60.0, -15.0), (100.0, 70.0), (140.0, 20.0), (179.0, -80.0), (-30.0, 85.0),
    ],
    990002: [
        (140.7, 0.0), (100.0, -30.0), (180.0, 40.0), (120.0, 60.0), (160.0, -60.0),
        (90.0, 10.0), (-160.0, 20.0), (130.0, 35.0), (150.0, -10.0), (110.0, -50.0),
    ],
}


# EPSG CRSs newer than EPSG_VERSION.
# Source: EPSG Geodetic Parameter Registry API, https://apps.epsg.org/api/v1/, change 2026.081,
# revision 2026-10-05. PROJ master data/sql at EPSG v13.105 holds the same rows.
# PROJ 9.9.0 (EPSG v13.102) and pyproj 3.8.0 predate them.
#   Datum 1471 Geocentric Datum of Malaysia 2020: GRS 1980 (7019), Greenwich, anchor epoch 2020.0.
#   Geographic 2D CRS 12287 GDM2020: extent 1151 Malaysia.
#   Transformation 12291 GDM2020 to WGS 84 (1): Geocentric translations 9603, 0 0 0 m, accuracy 1 m.
#   Projected CRSs 25314-25322: Cassini-Soldner (9806) conversions 12314-12322.
#   Origins are EPSG sexagesimal DMS (unit 9110). False easting and northing are 0 m.
# JUPEM Technical Guide, circular KPUP 3/2021, Table 7 prints the same GDM2020 origins.
SUPPLEMENT_SOURCE = "EPSG change 2026.081"
GDM2020_CODE = 12287
GDM2020_DATUM = "1471"
GDM2020_BOUNDS = (98.02, 0.85, 119.61, 7.81)
GDM2020_TO_WGS84 = "0,0,0"
GDM2020_TO_WGS84_ACCURACY = "1"
METHOD_CASSINI = "9806"
# (code, name, latitude of origin DMS, longitude of origin DMS, extent bounds W S E N)
GDM2020_GRIDS = (
    (25314, "GDM2020 / Johor Grid", "2.023319581", "103.333985196", (102.44, 1.21, 104.6, 2.95)),
    (25315, "GDM2020 / Sembilan and Melaka Grid", "2.424362944", "101.562294446",
     (101.7, 2.03, 102.71, 3.28)),
    (25316, "GDM2020 / Pahang Grid", "3.423868785", "102.260462219", (101.33, 2.45, 103.67, 4.78)),
    (25317, "GDM2020 / Selangor Grid", "3.40483731", "101.30244997", (100.76, 2.54, 101.97, 3.87)),
    (25318, "GDM2020 / Terengganu Grid", "4.564496715", "102.533701899",
     (102.38, 3.89, 103.72, 5.9)),
    (25319, "GDM2020 / Pinang Grid", "5.251519941", "100.204077228", (100.12, 5.12, 100.56, 5.59)),
    (25320, "GDM2020 / Kedah and Perlis Grid", "5.575281746", "100.381094996",
     (99.59, 5.08, 101.12, 6.72)),
    (25321, "GDM2020 / Perak Grid", "4.513264021", "100.485548363", (100.07, 3.66, 102.0, 5.92)),
    (25322, "GDM2020 / Kelantan Grid", "5.533707511", "102.103225823",
     (101.33, 4.54, 102.67, 6.29)),
)


class Unsupported(Exception):
    """A CRS that the Rust parser cannot represent exactly."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class Helmert:
    values: list[str]
    accuracy: str
    name: str


@dataclass
class Row:
    code: int
    kind: str
    name: str
    area: str
    accuracy: str
    datum: str
    proj: str
    method: str
    base_code: int
    bounds: tuple[float, float, float, float] | None
    pm_deg: float

    def line(self) -> str:
        return "\t".join(
            [str(self.code), self.kind, self.name, self.area, self.accuracy, self.datum, self.proj]
        )


@dataclass
class Table:
    rows: list[Row] = field(default_factory=list)
    skipped: dict[int, tuple[str, str]] = field(default_factory=dict)
    supported_methods: Counter = field(default_factory=Counter)


def fail(message: str) -> None:
    print(f"gen_epsg: {message}", file=sys.stderr)
    sys.exit(1)


def check_versions() -> str:
    """Stop unless pyproj, PROJ and the EPSG database match the pins."""
    epsg = get_database_metadata("EPSG.VERSION")
    found = (pyproj.__version__, pyproj.proj_version_str, epsg)
    wanted = (PYPROJ_VERSION, PROJ_VERSION, EPSG_VERSION)
    if found != wanted:
        fail(
            f"version mismatch: found pyproj {found[0]} PROJ {found[1]} EPSG {found[2]}, "
            f"expected pyproj {wanted[0]} PROJ {wanted[1]} EPSG {wanted[2]}. "
            f"Install with: pip install pyproj=={PYPROJ_VERSION}"
        )
    return epsg


def fmt(value: float) -> str:
    """Format a number with 15 significant digits and no trailing zeros."""
    text = f"{float(f'{value:.15g}')!r}"
    if text.endswith(".0"):
        text = text[:-2]
    return "0" if text == "-0" else text


def fmt_area(value: float) -> str:
    text = f"{round(value, 6):.6f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def parse_proj(text: str) -> list[tuple[str, str | None]]:
    tokens: list[tuple[str, str | None]] = []
    for token in text.split():
        key, sep, value = token.partition("=")
        tokens.append((key, value if sep else None))
    return tokens


def join_proj(tokens: list[tuple[str, str | None]]) -> str:
    return " ".join(key if value is None else f"{key}={value}" for key, value in tokens)


def area_points(bounds: tuple[float, float, float, float]) -> list[tuple[float, float]]:
    """Return the centre and the SW and NE corners inset by INSET."""
    west, south, east, north = bounds
    width = east - west if east >= west else east + 360.0 - west
    height = north - south

    def wrap(lon: float) -> float:
        return lon - 360.0 if lon > 180.0 else lon

    return [
        (wrap(west + width / 2.0), south + height / 2.0),
        (wrap(west + width * INSET), south + height * INSET),
        (wrap(west + width * (1.0 - INSET)), south + height * (1.0 - INSET)),
    ]


def finite(*values: float) -> bool:
    return all(math.isfinite(v) for v in values)


def flat_steps(op: dict) -> list[dict]:
    if op["type"] == "ConcatenatedOperation":
        return [s for step in op["steps"] for s in flat_steps(step)]
    return [op]


def helmert_only(op: dict) -> bool:
    """Return True when every transformation step uses an EPSG Helmert method.

    A single Helmert step maps directly.
    A chain of translation-only steps sums exactly, because each geocentric round trip cancels.
    Any other chain has no exact single Helmert.
    """
    codes = []
    for step in flat_steps(op):
        code = str(step.get("method", {}).get("id", {}).get("code", ""))
        if step["type"] == "Conversion" and code in NEUTRAL_CONVERSIONS:
            continue
        if step["type"] != "Transformation" or code not in HELMERT_METHODS:
            return False
        codes.append(code)
    if len(codes) == 1:
        return True
    return bool(codes) and all(code in HELMERT_TRANSLATION for code in codes)


def pipeline_values(definition: str) -> list[str]:
    """Return +towgs84 values from the PROJ pipeline that PROJ runs for a Helmert-only op.

    PROJJSON does not mark which steps PROJ inverts, so the pipeline is the source of truth.
    """
    helmerts = []
    for chunk in f" {definition}".split(" step ")[1:] if " step " in definition else [definition]:
        words = chunk.split()
        inverse = "inv" in words
        params = dict(w.split("=", 1) for w in words if "=" in w)
        name = params.pop("proj")
        if name in PIPELINE_NEUTRAL:
            continue
        if name != "helmert" or not set(params) <= HELMERT_PIPELINE_KEYS:
            raise Unsupported(f"Helmert-only operation runs as {chunk!r}")
        convention = params.pop("convention", "position_vector")
        values = [float(params.get(k, "0")) for k in ("x", "y", "z")]
        rotated = any(k in params for k in ("rx", "ry", "rz", "s"))
        if rotated:
            rotations = [float(params.get(k, "0")) for k in ("rx", "ry", "rz")]
            if convention == "coordinate_frame":
                rotations = [-r for r in rotations]
            values += rotations + [float(params.get("s", "0"))]
        # EPSG reverses a Helmert by negating every parameter.
        if inverse:
            values = [-v for v in values]
        helmerts.append(values)
    if not helmerts:
        return ["0", "0", "0"]
    if len(helmerts) == 1:
        return [fmt(v) for v in helmerts[0]]
    if any(len(values) != 3 for values in helmerts):
        raise Unsupported("chain of rotating Helmerts has no exact single Helmert")
    return [fmt(sum(values[i] for values in helmerts)) for i in range(3)]


class Generator:
    def __init__(self) -> None:
        self.wgs84 = CRS.from_epsg(4326)
        self.wgs84_proj = CRS.from_proj4(WGS84_PROJ)
        self.helmert_cache: dict[tuple, Helmert | None] = {}
        self.datum_cache: dict[int, str] = {}

    def datum_code(self, base_code: int) -> str:
        """Return the EPSG code of the datum of geographic CRS base_code, or "" when it has none."""
        if base_code not in self.datum_cache:
            ident = CRS.from_epsg(base_code).datum.to_json_dict().get("id") or {}
            code = str(ident["code"]) if ident.get("authority") == "EPSG" else ""
            self.datum_cache[base_code] = code
        return self.datum_cache[base_code]

    def helmert(self, base: CRS, base_code: int, bounds) -> Helmert | None:
        """Pick the first-ranked Helmert-only operation from base to EPSG:4326 for the area."""
        key = (base_code, bounds)
        if key in self.helmert_cache:
            return self.helmert_cache[key]
        aoi = AreaOfInterest(*bounds) if bounds else None
        group = TransformerGroup(base, self.wgs84, always_xy=True, area_of_interest=aoi)
        found = None
        for transformer in group.transformers:
            if not helmert_only(transformer.to_json_dict()):
                continue
            values = pipeline_values(transformer.definition)
            accuracy = "" if transformer.accuracy < 0 else fmt(transformer.accuracy)
            found = Helmert(values, accuracy, transformer.description)
            self.check_helmert(base, transformer, found, bounds)
            break
        self.helmert_cache[key] = found
        return found

    def check_helmert(self, base: CRS, transformer: Transformer, helmert: Helmert, bounds) -> None:
        """Stop when the emitted +towgs84 disagrees with the EPSG transformation."""
        base_tokens = [
            (k, v) for k, v in parse_proj(base.to_proj4()) if k not in ("+type",)
        ]
        tokens = []
        for k, v in base_tokens:
            if k == "+datum":
                tokens.append(("+ellps", DATUM_ELLPS[v]))
            elif k == "+pm":
                tokens.append(("+pm", fmt(pm_degrees(base))))
            else:
                tokens.append((k, v))
        tokens.insert(1, ("+towgs84", ",".join(helmert.values)))
        mine = Transformer.from_crs(
            CRS.from_proj4(join_proj(tokens)), self.wgs84_proj, always_xy=True, only_best=True
        )
        angle = base.axis_info[0].unit_conversion_factor * 180.0 / math.pi
        pm = pm_degrees(base)
        for lon, lat in area_points(bounds) if bounds else [(0.0, 0.0)]:
            ex, ey = transformer.transform((lon - pm) / angle, lat / angle)
            mx, my = mine.transform(lon - pm, lat)
            if not finite(ex, ey, mx, my):
                continue
            if abs(ex - mx) > DATUM_TOLERANCE_DEG or abs(ey - my) > DATUM_TOLERANCE_DEG:
                fail(
                    f"+towgs84={','.join(helmert.values)} disagrees with {helmert.name!r} "
                    f"at ({lon}, {lat}): EPSG ({ex}, {ey}), PROJ string ({mx}, {my})"
                )

    def build(self, code: int, kind: str) -> Row:
        crs = CRS.from_epsg(code)
        base = crs if kind == "geographic" else crs.geodetic_crs
        base_auth = base.to_authority(min_confidence=100)
        if base_auth is None or base_auth[0] != "EPSG":
            raise Unsupported("geographic base has no EPSG code")
        base_code = int(base_auth[1])
        method = "longlat" if kind == "geographic" else crs.coordinate_operation.method_code
        area = crs.area_of_use
        bounds = tuple(area.bounds) if area else None

        tokens = self.projection_tokens(crs, base, kind, method)
        proj_value = dict(tokens)["+proj"]
        allowed = frozenset({"longlat"}) if kind == "geographic" else METHOD_PROJ[method]
        if proj_value not in allowed:
            raise Unsupported(f"PROJ emits +proj={proj_value} for EPSG method {method}")

        is_wgs84 = ("+datum", "WGS84") in tokens
        tokens = self.normalise(tokens, crs, base)
        accuracy = ""
        if not is_wgs84:
            helmert = self.helmert(base, base_code, bounds)
            if helmert is not None:
                at = max(i for i, (k, _) in enumerate(tokens) if k in ELLIPSOID_KEYS) + 1
                tokens.insert(at, ("+towgs84", ",".join(helmert.values)))
                accuracy = helmert.accuracy

        for key, _ in tokens:
            if key not in ALLOWED_KEYS:
                raise Unsupported(f"key {key} outside the allowed list")
        if proj_value not in ALLOWED_PROJ:
            raise Unsupported(f"+proj={proj_value} outside the allowed list")

        row = Row(
            code=code,
            kind=kind,
            name=" ".join(crs.name.split()),
            area=",".join(fmt_area(v) for v in bounds) if bounds else "",
            accuracy=accuracy,
            datum=self.datum_code(base_code),
            proj=join_proj(tokens),
            method=method,
            base_code=base_code,
            bounds=bounds,
            pm_deg=pm_degrees(base),
        )
        if kind == "projected":
            self.check_projection(row, crs, base)
        return row

    def projection_tokens(self, crs: CRS, base: CRS, kind: str, method: str):
        if kind == "projected" and method not in METHOD_PROJ:
            op = crs.coordinate_operation
            raise Unsupported(f"EPSG method {method} {op.method_name} has no implemented +proj")
        if method == METHOD_PSEUDO_MERCATOR:
            if ("+datum", "WGS84") not in parse_proj(base.to_proj4()):
                raise Unsupported("Pseudo-Mercator on a datum other than WGS 84")
            return parse_proj(PSEUDO_MERCATOR_PROJ)
        if method == METHOD_LCC_WEST:
            return self.lcc_west_tokens(crs, base)
        if method == METHOD_POLAR_STEREO_C:
            return self.polar_stereo_c_tokens(crs, base)
        text = crs.to_proj4()
        if text is None:
            raise Unsupported("PROJ cannot export a PROJ string")
        tokens = parse_proj(text)
        if kind == "projected" and method not in NATIVE_AXIS_METHODS:
            tokens = add_axis(tokens, crs)
        return tokens

    def datum_tokens(self, base: CRS) -> list[tuple[str, str | None]]:
        return [
            (k, v)
            for k, v in parse_proj(base.to_proj4())
            if k not in ("+proj", "+no_defs", "+type")
        ]

    def lcc_west_tokens(self, crs: CRS, base: CRS):
        """Write the west-orientated LCC as LCC 1SP with a negated x axis.

        EPSG defines W = FE - E. PROJ applies +axis=wnu after the false easting.
        A false easting of -FE therefore yields -(E - FE) = FE - E.
        """
        p = conversion_params(crs)
        tokens = [
            ("+proj", "lcc"),
            ("+axis", "wnu"),
            ("+lat_1", fmt(p["8801"])),
            ("+lat_0", fmt(p["8801"])),
            ("+lon_0", fmt(p["8802"])),
            ("+k_0", fmt(p["8805"])),
            ("+x_0", fmt(-p["8806"])),
            ("+y_0", fmt(p["8807"])),
        ]
        return tokens + self.datum_tokens(base) + units_token(crs) + [("+no_defs", None)]

    def lcc_west_reference(self, crs: CRS, base: CRS):
        """Return an EPSG reference for the west-orientated LCC, which PROJ cannot instantiate.

        The reference is LCC 1SP with zero false easting, then W = FE - E in CRS units.
        """
        p = conversion_params(crs)
        unit = crs.axis_info[0].unit_conversion_factor
        tokens = [
            ("+proj", "lcc"),
            ("+lat_1", fmt(p["8801"])),
            ("+lat_0", fmt(p["8801"])),
            ("+lon_0", fmt(p["8802"])),
            ("+k_0", fmt(p["8805"])),
            ("+x_0", "0"),
            ("+y_0", fmt(p["8807"])),
        ]
        tokens += self.datum_tokens(base) + [("+units", "m"), ("+no_defs", None)]
        lcc = CRS.from_proj4(join_proj(tokens))
        east = Transformer.from_crs(lcc.geodetic_crs, lcc, always_xy=True)
        false_easting = p["8806"]

        class WestReference:
            @staticmethod
            def transform(lon: float, lat: float) -> tuple[float, float]:
                x, y = east.transform(lon, lat)
                return (false_easting - x) / unit, y / unit

        return WestReference()

    def polar_stereo_c_tokens(self, crs: CRS, base: CRS):
        """Write Polar Stereographic variant C as variant B with a shifted false northing.

        Variant C puts the false northing on the standard parallel at the origin meridian.
        That parallel lies at radius rho_F = a * m_F from the pole.
        """
        p = conversion_params(crs)
        lat_ts = p["8832"]
        ellipsoid = base.ellipsoid
        a = ellipsoid.semi_major_metre
        e2 = 1.0 - (ellipsoid.semi_minor_metre / a) ** 2
        phi = math.radians(lat_ts)
        rho_f = a * math.cos(phi) / math.sqrt(1.0 - e2 * math.sin(phi) ** 2)
        south = lat_ts < 0
        y_0 = p["8827"] - rho_f if south else p["8827"] + rho_f
        tokens = [
            ("+proj", "stere"),
            ("+lat_0", "-90" if south else "90"),
            ("+lat_ts", fmt(lat_ts)),
            ("+lon_0", fmt(p["8833"])),
            ("+x_0", fmt(p["8826"])),
            ("+y_0", repr(y_0)),
        ]
        return tokens + self.datum_tokens(base) + units_token(crs) + [("+no_defs", None)]

    def normalise(self, tokens, crs: CRS, base: CRS):
        """Rewrite tokens into forms the Rust parser reads, keeping the math unchanged."""
        out = []
        for key, value in tokens:
            if key == "+type":
                continue
            if key == "+datum" and value != "WGS84":
                if value not in DATUM_ELLPS:
                    raise Unsupported(f"PROJ datum {value} has no ellipsoid mapping")
                out.append(("+ellps", DATUM_ELLPS[value]))
            elif key == "+pm":
                out.append(("+pm", fmt(pm_degrees(base))))
            elif key == "+units" and value != "m":
                out.append(("+to_meter", fmt(crs.axis_info[0].unit_conversion_factor)))
            else:
                out.append((key, value))
        return out

    def check_projection(self, row: Row, crs: CRS, base: CRS) -> None:
        """Raise Unsupported when the PROJ string projects differently from EPSG."""
        mine_crs = CRS.from_proj4(row.proj)
        mine = Transformer.from_crs(mine_crs.geodetic_crs, mine_crs, always_xy=True)
        angle = base.axis_info[0].unit_conversion_factor * 180.0 / math.pi
        unit = crs.axis_info[0].unit_conversion_factor
        if row.method == METHOD_LCC_WEST:
            official = self.lcc_west_reference(crs, base)
            angle = 1.0
        elif row.method == METHOD_POLAR_STEREO_C:
            check_polar_stereo_c_example(crs, mine)
            return
        else:
            try:
                official = Transformer.from_crs(base, crs, always_xy=True)
            except pyproj.exceptions.ProjError as err:
                raise Unsupported(f"PROJ cannot instantiate the EPSG definition: {err}") from err
        compared = 0
        for lon, lat in area_points(row.bounds) if row.bounds else [(0.0, 0.0)]:
            ex, ey = official.transform((lon - row.pm_deg) / angle, lat / angle)
            mx, my = mine.transform(lon - row.pm_deg, lat)
            if not finite(ex, ey, mx, my):
                continue
            compared += 1
            if max(abs(ex - mx), abs(ey - my)) * unit > PROJECTION_TOLERANCE_M:
                raise Unsupported(
                    f"PROJ string differs from EPSG at ({lon}, {lat}): "
                    f"EPSG ({ex}, {ey}), PROJ string ({mx}, {my})"
                )
        if compared == 0:
            raise Unsupported("no finite check point")

    def generate(self) -> Table:
        table = Table()
        # The catalog lists a CRS once per area entry, so codes repeat.
        candidates = set()
        for info in query_crs_info(
            auth_name="EPSG", pj_types=PJType.GEOGRAPHIC_2D_CRS, allow_deprecated=False
        ):
            candidates.add((int(info.code), "geographic"))
        for info in query_crs_info(
            auth_name="EPSG", pj_types=PJType.PROJECTED_CRS, allow_deprecated=False
        ):
            crs = CRS.from_epsg(info.code)
            base = crs.geodetic_crs
            if base is not None and base.type_name == "Geographic 2D CRS":
                candidates.add((int(info.code), "projected"))
        for code, kind in sorted(candidates):
            crs = CRS.from_epsg(code)
            if kind == "geographic":
                method = "geographic"
            else:
                op = crs.coordinate_operation
                method = f"{op.method_code} {op.method_name}"
            try:
                row = self.build(code, kind)
            except Unsupported as err:
                table.skipped[code] = (method, err.reason)
                continue
            table.rows.append(row)
            table.supported_methods[method] += 1
        return table


def pm_degrees(base: CRS) -> float:
    pm = base.prime_meridian
    return float(fmt(pm.longitude * pm.unit_conversion_factor * 180.0 / math.pi))


def conversion_params(crs: CRS) -> dict[str, float]:
    """Return conversion parameters by EPSG code, angles in degrees and lengths in metres."""
    out = {}
    for param in crs.coordinate_operation.params:
        factor = param.unit_conversion_factor
        if param.unit_category == "angular":
            out[param.code] = param.value * factor * 180.0 / math.pi
        else:
            out[param.code] = param.value * factor
    return out


def check_polar_stereo_c_example(crs: CRS, mine: Transformer) -> None:
    """Check a Polar Stereographic C string against the EPSG Guidance Note 7-2 worked example.

    PROJ cannot instantiate variant C, so the published example is the only reference.
    The example uses International 1924, lat_ts -67, lon_0 140, FE 300000 and FN 200000.
    """
    p = conversion_params(crs)
    params = (round(p["8832"], 9), round(p["8833"], 9), p["8826"], p["8827"])
    if params != PS_C_EXAMPLE_PARAMS or crs.ellipsoid.name != PS_C_EXAMPLE_ELLIPSOID:
        raise Unsupported("Polar Stereographic C with no EPSG worked example to check against")
    x, y = mine.transform(*PS_C_EXAMPLE_LONLAT)
    ex, ey = PS_C_EXAMPLE_EN
    if max(abs(x - ex), abs(y - ey)) > PS_C_EXAMPLE_TOLERANCE_M:
        raise Unsupported(
            f"PROJ string gives ({x}, {y}) for the EPSG worked example, expected ({ex}, {ey})"
        )


def add_axis(tokens, crs: CRS):
    """Add the +axis that to_proj4 drops for some west- or south-pointing grids.

    PROJ keeps the EPSG axis order for these grids under always_xy.
    East/north grids and polar grids with two meridian axes need no +axis.
    """
    if any(key == "+axis" for key, _ in tokens):
        return tokens
    directions = tuple(axis.direction for axis in crs.axis_info[:2])
    if set(directions) == {"east", "north"} or directions[0] == directions[1]:
        return tokens
    letters = "".join(AXIS_LETTER[d] for d in directions) + "u"
    return [tokens[0], ("+axis", letters)] + tokens[1:]


def units_token(crs: CRS) -> list[tuple[str, str | None]]:
    factor = crs.axis_info[0].unit_conversion_factor
    return [("+units", "m")] if factor == 1.0 else [("+to_meter", fmt(factor))]


def sexagesimal_degrees(text: str) -> float:
    """Degrees from an EPSG sexagesimal DMS value DDD.MMSSsss (unit 9110)."""
    sign = -1.0 if text.startswith("-") else 1.0
    whole, _, frac = text.lstrip("-").partition(".")
    frac = (frac + "0000")[: max(len(frac), 4)]
    minutes = int(frac[:2])
    seconds = float(f"{frac[2:4]}.{frac[4:] or '0'}")
    return sign * (int(whole) + minutes / 60.0 + seconds / 3600.0)


def supplement_rows() -> list[Row]:
    """The SUPPLEMENT CRSs as rows of the built-in table format."""
    datum = f"+ellps=GRS80 +towgs84={GDM2020_TO_WGS84}"

    def area(bounds: tuple[float, float, float, float]) -> str:
        return ",".join(fmt_area(v) for v in bounds)

    rows = [
        Row(
            code=GDM2020_CODE,
            kind="geographic",
            name="GDM2020",
            area=area(GDM2020_BOUNDS),
            accuracy=GDM2020_TO_WGS84_ACCURACY,
            datum=GDM2020_DATUM,
            proj=f"+proj=longlat {datum} +no_defs",
            method="longlat",
            base_code=GDM2020_CODE,
            bounds=GDM2020_BOUNDS,
            pm_deg=0.0,
        )
    ]
    for code, name, lat, lon, bounds in GDM2020_GRIDS:
        proj = (
            f"+proj=cass +lat_0={fmt(sexagesimal_degrees(lat))} "
            f"+lon_0={fmt(sexagesimal_degrees(lon))} +x_0=0 +y_0=0 {datum} +units=m +no_defs"
        )
        rows.append(
            Row(
                code=code,
                kind="projected",
                name=name,
                area=area(bounds),
                accuracy=GDM2020_TO_WGS84_ACCURACY,
                datum=GDM2020_DATUM,
                proj=proj,
                method=METHOD_CASSINI,
                base_code=GDM2020_CODE,
                bounds=bounds,
                pm_deg=0.0,
            )
        )
    return rows


def check_supplement(table: Table, rows: list[Row]) -> list[str]:
    """Report SUPPLEMENT rows the pinned database holds, and rows the Rust parser cannot read."""
    errors = []
    codes = {row.code for row in table.rows}
    previous = 0
    for row in rows:
        if row.code <= previous:
            errors.append(f"SUPPLEMENT is not sorted at EPSG:{row.code}")
        previous = row.code
        if row.code in codes:
            errors.append(f"EPSG:{row.code} is in epsg.tsv: remove it from SUPPLEMENT")
        try:
            CRS.from_epsg(row.code)
            errors.append(f"EPSG:{row.code} is in EPSG {EPSG_VERSION}: remove it from SUPPLEMENT")
        except pyproj.exceptions.CRSError:
            pass
        tokens = parse_proj(row.proj)
        if dict(tokens)["+proj"] not in ALLOWED_PROJ or any(k not in ALLOWED_KEYS for k, _ in tokens):
            errors.append(f"EPSG:{row.code} PROJ string is outside the allowed list: {row.proj}")
        CRS.from_proj4(row.proj)
    return errors


def render_supplement(rows: list[Row]) -> str:
    lines = [
        f"# EPSG supplement: {SUPPLEMENT_SOURCE}, absent from EPSG {EPSG_VERSION}. "
        f"PROJ {pyproj.proj_version_str}",
        "code\tkind\tname\tarea\taccuracy_m\tdatum\tproj",
    ]
    lines.extend(row.line() for row in rows)
    return "\n".join(lines) + "\n"


def render_tsv(epsg: str, table: Table) -> str:
    lines = [
        f"# EPSG {epsg} PROJ {pyproj.proj_version_str}",
        "code\tkind\tname\tarea\taccuracy_m\tdatum\tproj",
    ]
    lines.extend(row.line() for row in table.rows)
    return "\n".join(lines) + "\n"


def fixture_line(src, dst, x_in, y_in, x_out, y_out) -> str:
    return "\t".join([str(src), str(dst), repr(x_in), repr(y_in), repr(x_out), repr(y_out)])


def render_fixtures(epsg: str, table: Table, supplement: list[Row]) -> str:
    """Reference points computed from the emitted PROJ strings."""
    wgs84 = CRS.from_proj4(WGS84_PROJ)
    lines = [
        f"# EPSG {epsg} PROJ {pyproj.proj_version_str} pyproj {pyproj.__version__}",
        "# Built from the PROJ strings in epsg.tsv and epsg_supplement.tsv.",
        "# Forward rows use the CRS's own geographic base.",
        "# A forward row applies no datum shift, even when the base row has another +towgs84.",
        "src\tdst\tx_in\ty_in\tx_out\ty_out",
    ]
    for row in table.rows + supplement:
        if row.kind != "projected" or row.bounds is None:
            continue
        crs = CRS.from_proj4(row.proj)
        forward = Transformer.from_crs(crs.geodetic_crs, crs, always_xy=True, only_best=True)
        cross = None
        if "+towgs84=" in row.proj:
            cross = Transformer.from_crs(crs, wgs84, always_xy=True, only_best=True)
        for lon, lat in area_points(row.bounds):
            x, y = forward.transform(lon - row.pm_deg, lat)
            if not finite(x, y):
                continue
            lines.append(fixture_line(row.base_code, row.code, lon - row.pm_deg, lat, x, y))
            if cross is not None:
                wlon, wlat = cross.transform(x, y)
                if finite(wlon, wlat):
                    lines.append(fixture_line(row.code, 4326, x, y, wlon, wlat))
    for code, text in SYNTHETIC.items():
        crs = CRS.from_proj4(text)
        to_grid = Transformer.from_crs(wgs84, crs, always_xy=True, only_best=True)
        to_wgs84 = Transformer.from_crs(crs, wgs84, always_xy=True, only_best=True)
        for lon, lat in SYNTHETIC_POINTS[code]:
            x, y = to_grid.transform(lon, lat)
            if not finite(x, y):
                continue
            lines.append(fixture_line(4326, code, lon, lat, x, y))
            blon, blat = to_wgs84.transform(x, y)
            if finite(blon, blat):
                lines.append(fixture_line(code, 4326, x, y, blon, blat))
    return "\n".join(lines) + "\n"


def malaysia_points(bounds: tuple[float, float, float, float]) -> list[tuple[float, float]]:
    """Spread MALAYSIA_POINTS over the parts of each region inside the area of use."""
    west, south, east, north = bounds
    boxes = []
    for rw, rs, re, rn in MALAYSIA_REGIONS:
        box = (max(rw, west), max(rs, south), min(re, east), min(rn, north))
        if box[0] < box[2] and box[1] < box[3]:
            boxes.append(box)
    if not boxes:
        return []
    counts = [MALAYSIA_POINTS // len(boxes)] * len(boxes)
    for i in range(MALAYSIA_POINTS - sum(counts)):
        counts[i] += 1
    points = []
    golden = (math.sqrt(5.0) - 1.0) / 2.0
    for (bw, bs, be, bn), count in zip(boxes, counts):
        for i in range(count):
            fx = INSET + (1.0 - 2.0 * INSET) * (i + 0.5) / count
            fy = INSET + (1.0 - 2.0 * INSET) * ((0.5 + i * golden) % 1.0)
            points.append((round(bw + (be - bw) * fx, 6), round(bs + (bn - bs) * fy, 6)))
    return points


def render_malaysia(epsg: str, table: Table) -> str:
    """Reference points computed from the official EPSG definitions, not PROJ strings.

    Each row also runs through the epsg.tsv strings, the way nodedb-crs does.
    A row whose official operation differs from the epsg.tsv Helmert gets tol_m.
    tol_m is the accuracy of that Helmert, and the row must agree within it.
    """
    rows = {row.code: row for row in table.rows}
    header = [
        f"# EPSG {epsg} PROJ {pyproj.proj_version_str} pyproj {pyproj.__version__}",
        "# Built from official EPSG codes with only_best=True. One op line per operation PROJ used.",
        "# tol_m empty: the epsg.tsv strings reproduce the row within "
        f"{MALAYSIA_MATCH_M} m projected or {MALAYSIA_MATCH_DEG} degrees geographic.",
        "# tol_m set: PROJ used another operation than the epsg.tsv Helmert.",
        "# tol_m is that Helmert's accuracy, metres, against max(|dlon|, |dlat|) * "
        f"{METRES_PER_DEGREE_BOUND:g}.",
    ]
    body = []
    wgs84 = CRS.from_epsg(4326)
    wgs84_proj = CRS.from_proj4(WGS84_PROJ)
    for code in MALAYSIA_CODES:
        crs = CRS.from_epsg(code)
        if code == 4326:
            continue
        base = crs.geodetic_crs if crs.is_projected else crs
        base_code = int(base.to_authority(min_confidence=100)[1])
        points = malaysia_points(tuple(crs.area_of_use.bounds))
        mine = CRS.from_proj4(rows[code].proj)
        pairs = []
        if crs.is_projected:
            # nodedb-crs runs no datum shift between a CRS and its own base.
            pairs.append((base_code, code, base, crs, mine.geodetic_crs, mine))
        if base_code != 4326:
            pairs.append((code, 4326, crs, wgs84, mine, wgs84_proj))
        transformers = []
        for src, dst, s, d, ms, md in pairs:
            t = Transformer.from_crs(s, d, always_xy=True, only_best=True)
            m = Transformer.from_crs(ms, md, always_xy=True, only_best=True)
            transformers.append((src, dst, t, m, {}))
        for lon, lat in points:
            x_in, y_in = lon, lat
            for src, dst, t, m, used in transformers:
                x, y = t.transform(x_in, y_in)
                if not finite(x, y):
                    break
                # PROJ picks the operation per point when several apply.
                # A transformer with one operation has no last-used record.
                try:
                    op = t.get_last_used_operation()
                except pyproj.exceptions.ProjError:
                    op = t
                used[f"{op.description} | {op.definition}"] = None
                mx, my = m.transform(x_in, y_in)
                tol = malaysia_tolerance(rows[src if dst == 4326 else dst], dst, (x, y), (mx, my))
                body.append(fixture_line(src, dst, x_in, y_in, x, y) + "\t" + tol)
                x_in, y_in = x, y
        for src, dst, _, _, used in transformers:
            for op in used:
                header.append(f"# op {src} {dst}: {op}")
    lines = header + ["src\tdst\tx_in\ty_in\tx_out\ty_out\ttol_m"] + body
    return "\n".join(lines) + "\n"


def malaysia_tolerance(row: Row, dst: int, official, mine) -> str:
    """Return "" when the epsg.tsv strings reproduce the official output, else the Helmert accuracy.

    Stop when they disagree by more than that accuracy, or by anything on a projected target.
    """
    dx, dy = abs(official[0] - mine[0]), abs(official[1] - mine[1])
    if dst != 4326:
        if max(dx, dy) * rows_unit(row) > MALAYSIA_MATCH_M:
            fail(f"EPSG:{dst} projects ({mine}) from epsg.tsv, official ({official})")
        return ""
    if max(dx, dy) <= MALAYSIA_MATCH_DEG:
        return ""
    off_m = max(dx, dy) * METRES_PER_DEGREE_BOUND
    if not row.accuracy or off_m > float(row.accuracy):
        fail(
            f"EPSG:{row.code} to WGS 84 is {off_m} m from the official operation, "
            f"beyond the epsg.tsv Helmert accuracy {row.accuracy!r}"
        )
    return row.accuracy


def rows_unit(row: Row) -> float:
    """Metres per unit of a projected epsg.tsv row."""
    return CRS.from_proj4(row.proj).axis_info[0].unit_conversion_factor


def summarise(table: Table) -> None:
    kinds = Counter(row.kind for row in table.rows)
    projs = Counter(dict(parse_proj(row.proj))["+proj"] for row in table.rows)
    no_helmert = [
        row for row in table.rows if "+towgs84=" not in row.proj and "+datum=WGS84" not in row.proj
    ]
    print(f"rows: {len(table.rows)}")
    for kind, count in sorted(kinds.items()):
        print(f"  {kind}: {count}")
    print("rows per +proj:")
    for name, count in sorted(projs.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {name}: {count}")
    print(f"rows with no Helmert to WGS 84: {len(no_helmert)}")
    print("rows per EPSG method:")
    for method, count in sorted(table.supported_methods.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {method}: {count}")
    reasons = Counter((method, reason) for method, reason in table.skipped.values())
    print(f"skipped codes: {len(table.skipped)}")
    for (method, reason), count in sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0])):
        codes = [c for c, v in table.skipped.items() if v == (method, reason)]
        listed = " ".join(str(c) for c in codes)
        print(f"  {count} [{method}] {reason}: {listed}")


def unlisted_skips(table: Table) -> list[str]:
    errors = []
    for code, (method, reason) in sorted(table.skipped.items()):
        if code not in SKIP:
            errors.append(f"EPSG:{code} [{method}] skipped and not in SKIP: {reason}")
    for code in sorted(SKIP):
        if code not in table.skipped:
            errors.append(f"EPSG:{code} is in SKIP but generates a row: remove it from SKIP")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="compare with the committed files")
    mode.add_argument("--fixtures", action="store_true", help="also write the reference fixtures")
    args = parser.parse_args()

    epsg = check_versions()
    table = Generator().generate()
    summarise(table)
    supplement = supplement_rows()
    print(f"supplement rows: {len(supplement)}")
    outputs = {TSV_PATH: render_tsv(epsg, table), SUPPLEMENT_PATH: render_supplement(supplement)}
    if args.fixtures or args.check:
        outputs[FIXTURE_PATH] = render_fixtures(epsg, table, supplement)
        outputs[MALAYSIA_PATH] = render_malaysia(epsg, table)

    errors = unlisted_skips(table) + check_supplement(table, supplement)
    if args.check:
        for path, text in outputs.items():
            if not path.exists():
                errors.append(f"{path.relative_to(ROOT)} is missing: run scripts/gen_epsg.py --fixtures")
            elif path.read_text() != text:
                errors.append(f"{path.relative_to(ROOT)} differs: run scripts/gen_epsg.py --fixtures")
    else:
        for path, text in outputs.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
            print(f"wrote {path.relative_to(ROOT)}")
    for error in errors:
        print(f"gen_epsg: {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
