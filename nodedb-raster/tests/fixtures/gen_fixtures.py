#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Generate the TIFF fixtures of the nodedb-raster file tests.

Run from any directory:

    python -I gen_fixtures.py [output-dir]

The output directory defaults to the directory of this script. It needs
numpy, tifffile, imagecodecs and rasterio (GDAL 3.8 or later). The script
writes every fixture, checks each one by reading it back with GDAL or
tifffile, and writes manifest.tsv.

Pattern fixtures hold value(band, y, x) of the counter
band * 97 + y * 31 + x * 7 + 1, cast with wrapping to the sample type
after the per-type offset below (floats: counter * 0.25 - 40). The Rust
tests compute the same pattern. Other fixtures store the expected samples
GDAL decodes as <id>.raw.z (band-sequential, little-endian, zlib) and the
expected mask as <id>.mask.z (one byte per pixel, 1 for valid, zlib).
"""

import os
import sys
import warnings
import zlib

import numpy as np
import rasterio
import rasterio.shutil
import tifffile
from rasterio.transform import from_origin

warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)

OUT = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(__file__))
OFFSETS = {"uint8": 0, "int8": 60, "uint16": 0, "int16": 300, "uint32": 0, "int32": 70000}
ROWS = []


def pattern(bands, height, width, dtype):
    b, y, x = np.meshgrid(np.arange(bands), np.arange(height), np.arange(width), indexing="ij")
    counter = (b * 97 + y * 31 + x * 7 + 1).astype(np.int64)
    if dtype in ("float32", "float64"):
        return (counter * 0.25 - 40.0).astype(dtype)
    bits = np.dtype(dtype).itemsize * 8
    wrapped = (counter - OFFSETS[dtype]) % (1 << bits)
    unsigned = wrapped.astype(np.uint64).astype(f"uint{bits}")
    return unsigned.view(dtype)


def path(name):
    return os.path.join(OUT, name)


def row(rid, file, image, dtype, width, height, bands, expect="pattern", mask="-", tol=0):
    ROWS.append((rid, file, str(image), dtype, str(width), str(height), str(bands), expect, mask, str(tol)))


def write_z(name, raw):
    with open(path(name), "wb") as out:
        out.write(zlib.compress(raw, 9))
    return name


def write_raw(rid, data):
    raw = np.ascontiguousarray(data.astype(data.dtype.newbyteorder("<"))).tobytes()
    return write_z(f"{rid}.raw.z", raw)


def write_mask(rid, mask):
    return write_z(f"{rid}.mask.z", (np.asarray(mask) != 0).astype(np.uint8).tobytes())


def matrix():
    """Every stream codec x predictor x strips/tiles x classic/BigTIFF x
    chunky/planar, alternating the byte order."""
    width, height, bands = 20, 18, 2
    codecs = {
        "none": None,
        "lzw": "lzw",
        "deflate": "adobe_deflate",
        "packbits": "packbits",
        "zstd": "zstd",
    }
    flip = False
    for codec, tf_codec in codecs.items():
        predictors = [("p1", None, "uint16")]
        if codec in ("lzw", "deflate", "zstd"):
            predictors += [("p2", "horizontal", "uint16"), ("p3", "floatingpoint", "float32")]
        for pname, predictor, dtype in predictors:
            for layout in ("strip", "tile"):
                for big in (False, True):
                    for planar in ("contig", "separate"):
                        flip = not flip
                        order = ">" if flip else "<"
                        rid = f"m-{codec}-{pname}-{layout}-{'big' if big else 'classic'}-{planar}-{'be' if flip else 'le'}"
                        data = pattern(bands, height, width, dtype)
                        array = data if planar == "separate" else np.moveaxis(data, 0, -1)
                        kwargs = dict(
                            bigtiff=big,
                            byteorder=order,
                            photometric="minisblack",
                            planarconfig=planar,
                            compression=tf_codec,
                            predictor=predictor,
                            metadata=None,
                        )
                        if layout == "tile":
                            kwargs["tile"] = (16, 16)
                        else:
                            kwargs["rowsperstrip"] = 5
                        tifffile.imwrite(path(rid + ".tif"), array, **kwargs)
                        back = tifffile.imread(path(rid + ".tif"))
                        back = back if planar == "separate" else np.moveaxis(back, -1, 0)
                        assert np.array_equal(back, data), rid
                        row(rid, rid + ".tif", 0, dtype, width, height, bands)


def sample_types():
    """Each sample type once, deflate tiles."""
    width, height = 20, 18
    for dtype in ("uint8", "int8", "int16", "uint32", "int32", "float64"):
        rid = f"t-{dtype}"
        data = pattern(1, height, width, dtype)
        tifffile.imwrite(
            path(rid + ".tif"), data[0], tile=(16, 16), compression="adobe_deflate",
            photometric="minisblack", metadata=None,
        )
        assert np.array_equal(tifffile.imread(path(rid + ".tif")), data[0]), rid
        row(rid, rid + ".tif", 0, dtype, width, height, 1)


def gdal_write(name, data, **profile):
    bands, height, width = data.shape
    profile = dict(driver="GTiff", width=width, height=height, count=bands, dtype=data.dtype.name, **profile)
    with rasterio.open(path(name), "w", **profile) as dst:
        dst.write(data)


def lerc():
    """LERC, LERC_DEFLATE and LERC_ZSTD, chunky and planar, strips and
    tiles; a float band with NaN gives a LERC mask."""
    width, height, bands = 20, 18, 2
    for comp in ("LERC", "LERC_DEFLATE", "LERC_ZSTD"):
        for interleave in ("pixel", "band"):
            for tiled in (True, False):
                rid = f"l-{comp.lower()}-{interleave}-{'tile' if tiled else 'strip'}"
                data = pattern(bands, height, width, "uint16")
                extra = dict(tiled=True, blockxsize=16, blockysize=16) if tiled else dict(blockysize=5)
                gdal_write(rid + ".tif", data, compress=comp, max_z_error=0, interleave=interleave, **extra)
                with rasterio.open(path(rid + ".tif")) as src:
                    assert np.array_equal(src.read(), data), rid
                row(rid, rid + ".tif", 0, "uint16", width, height, bands)
        rid = f"l-{comp.lower()}-mask"
        data = pattern(1, height, width, "float32")
        data[0, 3:6, 4:9] = np.nan
        gdal_write(rid + ".tif", data, compress=comp, max_z_error=0, tiled=True, blockxsize=16, blockysize=16)
        valid = ~np.isnan(data[0])
        filled = np.where(valid, data[0], 0).astype("float32")
        row(rid, rid + ".tif", 0, "float32", width, height, 1, write_raw(rid, filled[None]), write_mask(rid, valid))
    rid = "l-lerc-bigtiff"
    data = pattern(1, 18, 20, "int16")
    gdal_write(rid + ".tif", data, compress="LERC", max_z_error=0, tiled=True, blockxsize=16, blockysize=16, BIGTIFF="YES")
    row(rid, rid + ".tif", 0, "int16", 20, 18, 1)


def smooth(bands, height, width):
    b, y, x = np.meshgrid(np.arange(bands), np.arange(height), np.arange(width), indexing="ij")
    return (60 + 40 * np.sin(x / 5.0 + b) + 30 * np.cos(y / 4.0 - b)).astype("uint8")


def jpeg():
    """JPEG YCbCr tiles with JPEGTables, gray strips, and RGB tiles. The
    expected samples are GDAL's (libjpeg) decode."""
    cases = [
        ("j-ycbcr-tile", 3, dict(tiled=True, blockxsize=16, blockysize=16, photometric="YCBCR")),
        ("j-gray-strip", 1, dict(blockysize=8)),
        ("j-rgb-tile", 3, dict(tiled=True, blockxsize=16, blockysize=16, photometric="RGB")),
    ]
    for rid, bands, extra in cases:
        data = smooth(bands, 24, 40)
        gdal_write(rid + ".tif", data, compress="JPEG", jpeg_quality=90, interleave="pixel", **extra)
        with rasterio.open(path(rid + ".tif")) as src:
            decoded = src.read()
        row(rid, rid + ".tif", 0, "uint8", 40, 24, bands, write_raw(rid, decoded), tol=3)


def cog():
    """A COG with two overview levels and an internal mask."""
    width, height = 64, 48
    data = pattern(1, height, width, "uint16")
    mask = np.full((height, width), 255, dtype=np.uint8)
    mask[10:20, 30:50] = 0
    src_name = path("cog-src.tif")
    profile = dict(
        driver="GTiff", width=width, height=height, count=1, dtype="uint16",
        crs="EPSG:32648", transform=from_origin(600000, 450000, 30, 30),
        tiled=True, blockxsize=16, blockysize=16,
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with rasterio.open(src_name, "w", **profile) as dst:
            dst.write(data)
            dst.write_mask(mask)
        rasterio.shutil.copy(
            src_name, path("cog-mask.tif"), driver="COG", COMPRESS="DEFLATE", PREDICTOR="2",
            BLOCKSIZE="16", OVERVIEWS="AUTO", RESAMPLING="NEAREST", OVERVIEW_COUNT="2",
        )
    os.remove(src_name)
    for level in (None, 0, 1):
        image = 0 if level is None else level + 1
        with rasterio.open(path("cog-mask.tif"), overview_level=level) as src:
            values = src.read()
            valid = src.read_masks(1)
            h, w = src.height, src.width
        rid = f"cog-mask-{image}"
        expect = "pattern" if image == 0 else write_raw(rid, values)
        row(rid, "cog-mask.tif", image, "uint16", w, h, 1, expect, write_mask(rid, valid))


def landsat():
    """A Landsat Collection 2 Level-2 shaped band: uint16 COG, 256x256
    deflate tiles with predictor 2, overviews, nodata 0, UTM, 30 m."""
    width, height = 300, 260
    y, x = np.mgrid[0:height, 0:width]
    data = (7273 + 3 * x + 2 * y).astype("uint16")[None]
    data[0, :, :12] = 0
    src_name = path("landsat-src.tif")
    profile = dict(
        driver="GTiff", width=width, height=height, count=1, dtype="uint16", nodata=0,
        crs="EPSG:32648", transform=from_origin(399960, 5000040, 30, 30),
    )
    with rasterio.open(src_name, "w", **profile) as dst:
        dst.write(data)
        dst.scales = (2.75e-05,)
        dst.offsets = (-0.2,)
        dst.set_band_description(1, "SR_B4")
    rasterio.shutil.copy(
        src_name, path("landsat-c2.tif"), driver="COG", COMPRESS="DEFLATE", PREDICTOR="2",
        BLOCKSIZE="256", OVERVIEWS="AUTO", RESAMPLING="NEAREST",
    )
    os.remove(src_name)
    with rasterio.open(path("landsat-c2.tif")) as src:
        assert src.overviews(1), "landsat fixture has overviews"
        assert np.array_equal(src.read(), data)
    row("landsat-c2", "landsat-c2.tif", 0, "uint16", width, height, 1, write_raw("landsat-c2", data))
    with rasterio.open(path("landsat-c2.tif"), overview_level=0) as src:
        row("landsat-c2-1", "landsat-c2.tif", 1, "uint16", src.width, src.height, 1, write_raw("landsat-c2-1", src.read()))


def metadata():
    """Per-band GDAL_METADATA: description, scale, offset, units, a band
    nodata item and a valid range, over a shared GDAL_NODATA."""
    data = pattern(3, 6, 8, "int16")
    name = path("gdal-metadata.tif")
    profile = dict(driver="GTiff", width=8, height=6, count=3, dtype="int16", nodata=-1)
    with rasterio.open(name, "w", **profile) as dst:
        dst.write(data)
        dst.scales = (0.01, 1.0, 0.5)
        dst.offsets = (273.15, 0.0, -10.0)
        dst.units = ("K", "", "m")
        for band, desc in enumerate(("temperature", "quality", "height"), start=1):
            dst.set_band_description(band, desc)
        dst.update_tags(2, NODATA="-9999")
        dst.update_tags(3, valid_range="0,1000")
    tags = tifffile.TiffFile(name).pages[0].tags
    assert 42112 in tags and 42113 in tags
    row("gdal-metadata", "gdal-metadata.tif", 0, "int16", 8, 6, 3)


def main():
    os.makedirs(OUT, exist_ok=True)
    matrix()
    sample_types()
    lerc()
    jpeg()
    cog()
    landsat()
    metadata()
    with open(path("manifest.tsv"), "w") as out:
        out.write("# id\tfile\timage\tdtype\twidth\theight\tbands\texpect\tmask\ttol\n")
        for fields in ROWS:
            out.write("\t".join(fields) + "\n")
    print(f"{len(ROWS)} fixtures in {OUT}")


if __name__ == "__main__":
    main()
