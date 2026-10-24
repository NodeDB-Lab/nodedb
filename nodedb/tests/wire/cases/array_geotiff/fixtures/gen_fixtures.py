#!/usr/bin/env python3
# SPDX-License-Identifier: BUSL-1.1
"""Generate the GeoTIFF fixtures of the remote and multi-source COPY wire tests.

Run from any directory:

    python -I gen_fixtures.py [output-dir]

The output directory defaults to the directory of this script. It needs
numpy and rasterio (GDAL 3.8 or later). Every grid is 256 x 192 pixels of
30 m in EPSG:32647 with its corner at (600000, 700000), the grid of the
in-test scene fixture.

- band-b4.tif, band-b5.tif: uint16 COGs in 64 x 64 deflate tiles, nodata 0.
  Band k (4 is 1, 5 is 2) holds 1000 k + (7 r + 13 c) mod 997, and 0 in
  the block rows 100..110, cols 120..130.
- band-qa.tif: uint16 COG, no nodata, 1 + (r + c) mod 251.
- band-b5-tiles128.tif: band-b5 in 128 x 128 tiles (another block layout).
- band-b5-shifted.tif: band-b5 one pixel east (another geotransform).
- scene-overview.tif: band-b4 values, no nodata, with nearest overviews
  at factors 2 and 4.
- adopt-metadata.tif: 64 x 64 uint16, nodata 0, SCALE 2.75e-05,
  OFFSET -0.2, UNITS reflectance in GDAL_METADATA.
- expected.tsv: values the tests read back that GDAL computes (overview
  sizes and sums).
"""

import os
import sys
import warnings

import numpy as np
import rasterio
import rasterio.shutil
from rasterio.enums import Resampling
from rasterio.transform import from_origin

warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)

OUT = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(__file__))
WIDTH, HEIGHT = 256, 192
CRS = "EPSG:32647"
TRANSFORM = from_origin(600000, 700000, 30, 30)


def path(name):
    return os.path.join(OUT, name)


def band(k, nodata_block=True):
    r, c = np.mgrid[0:HEIGHT, 0:WIDTH]
    data = (1000 * k + (7 * r + 13 * c) % 997).astype("uint16")
    if nodata_block:
        data[100:110, 120:130] = 0
    return data


def qa():
    r, c = np.mgrid[0:HEIGHT, 0:WIDTH]
    return (1 + (r + c) % 251).astype("uint16")


def cog(name, data, nodata, block=64, transform=TRANSFORM, overviews=None):
    """Write `data` as a deflate COG of `block` tiles."""
    src = path(name + ".src.tif")
    profile = dict(
        driver="GTiff", width=data.shape[1], height=data.shape[0], count=1, dtype="uint16",
        crs=CRS, transform=transform, nodata=nodata, tiled=True, blockxsize=block,
        blockysize=block,
    )
    with rasterio.open(src, "w", **profile) as dst:
        dst.write(data[None])
        if overviews:
            dst.build_overviews(overviews, Resampling.nearest)
    options = dict(COMPRESS="DEFLATE", PREDICTOR="2", BLOCKSIZE=str(block))
    if overviews:
        options["OVERVIEWS"] = "FORCE_USE_EXISTING"
    else:
        options["OVERVIEWS"] = "NONE"
    rasterio.shutil.copy(src, path(name), driver="COG", **options)
    os.remove(src)
    with rasterio.open(path(name)) as check:
        assert check.width == data.shape[1] and check.height == data.shape[0], name
        assert (check.read(1) == data).all(), name


def adopt_metadata():
    data = (1000 + np.arange(64 * 64) % 997).astype("uint16").reshape(64, 64)
    data[0, :8] = 0
    profile = dict(
        driver="GTiff", width=64, height=64, count=1, dtype="uint16", crs=CRS,
        transform=TRANSFORM, nodata=0, tiled=True, blockxsize=64, blockysize=64,
        compress="deflate",
    )
    with rasterio.open(path("adopt-metadata.tif"), "w", **profile) as dst:
        dst.write(data[None])
        dst.scales = (2.75e-05,)
        dst.offsets = (-0.2,)
        dst.units = ("reflectance",)
    with rasterio.open(path("adopt-metadata.tif")) as check:
        assert check.scales == (2.75e-05,) and check.offsets == (-0.2,), check.tags(1)
        assert check.units == ("reflectance",)


def main():
    os.makedirs(OUT, exist_ok=True)
    cog("band-b4.tif", band(1), 0)
    cog("band-b5.tif", band(2), 0)
    cog("band-qa.tif", qa(), None)
    cog("band-b5-tiles128.tif", band(2), 0, block=128)
    cog("band-b5-shifted.tif", band(2), 0, transform=from_origin(600030, 700000, 30, 30))
    cog("scene-overview.tif", band(1, nodata_block=False), None, overviews=[2, 4])
    adopt_metadata()
    expected = []
    for level in (0, 1):
        with rasterio.open(path("scene-overview.tif"), overview_level=level) as src:
            values = src.read(1).astype(np.int64)
            image = level + 1
            expected.append((f"overview{image}_width", src.width))
            expected.append((f"overview{image}_height", src.height))
            expected.append((f"overview{image}_sum", int(values.sum())))
            expected.append((f"overview{image}_first", int(values[0, 0])))
    with open(path("expected.tsv"), "w") as out:
        for key, value in expected:
            out.write(f"{key}\t{value}\n")
    print(f"fixtures in {OUT}")


if __name__ == "__main__":
    main()
