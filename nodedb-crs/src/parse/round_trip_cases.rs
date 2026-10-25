// SPDX-License-Identifier: Apache-2.0

//! PROJ strings that cover every method, flag, unit, axis order and datum
//! form, for the writer round-trip tests.

pub(crate) const CASES: &[&str] = &[
    "+proj=longlat +datum=WGS84 +no_defs",
    "+proj=utm +zone=48 +datum=WGS84 +units=m +no_defs",
    "+proj=utm +zone=50 +south +ellps=GRS80 +towgs84=0,0,0 +units=m +no_defs",
    "+proj=omerc +no_uoff +lat_0=4 +lonc=102.25 +alpha=323.025796466667 \
     +gamma=323.130102361111 +k=0.99984 +x_0=804671 +y_0=0 +ellps=GRS80 +units=m +no_defs",
    "+proj=omerc +lat_0=4 +lonc=115 +alpha=53.31580995 +k=0.99984 +no_rot",
    "+proj=tmerc +lat_0=4 +lon_0=102 +k=0.99995 +x_0=40000 +y_0=-10000 \
     +a=6377304.063 +rf=300.8017 +pm=paris +towgs84=-11,851,5,0.1,-0.2,0.3,1.5 \
     +to_meter=0.3047972654",
    "+proj=longlat +R=6371007.181 +no_defs",
    "+proj=tmerc +a=6378000 +rf=297.5 +units=us-ft",
    "+proj=tmerc +axis=wsu +lon_0=17 +ellps=WGS84",
    "+proj=longlat +axis=wnu +datum=WGS84",
    "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=23 +lon_0=-96 +x_0=10 +y_0=-20",
    "+proj=lcc +axis=wnu +lat_1=65 +lat_0=65 +lon_0=-18 +k_0=1 +x_0=-500000 +ellps=intl",
    "+proj=merc +lat_ts=10 +lon_0=3",
    "+proj=merc +k=0.997 +lon_0=110",
    "+proj=webmerc +datum=WGS84",
    "+proj=cea +lat_ts=30 +ellps=WGS84",
    "+proj=cea +k_0=1 +R=6371000",
    "+proj=eqc +lat_ts=12 +lat_0=1",
    "+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45",
    "+proj=stere +lat_0=-90 +lon_0=0 +k=0.994",
    "+proj=stere +lat_0=52 +lon_0=5 +k=0.9999",
    "+proj=ups +south +ellps=WGS84",
    "+proj=sterea +lat_0=52.15 +lon_0=5.38 +k=0.9999079 +x_0=155000 +y_0=463000",
    "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96",
    "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000",
    "+proj=cass +lat_0=4 +lon_0=101 +hyperbolic",
    "+proj=cass +axis=swu +lat_0=48 +lon_0=31.8 +pm=-17.6666666666667",
    "+proj=poly +lat_0=1 +lon_0=-54",
    "+proj=aeqd +guam +lat_0=13.47 +lon_0=144.75",
    "+proj=aeqd +lat_0=90",
    "+proj=sinu +R=6371007.181",
    "+proj=eqearth +lon_0=150",
    "+proj=krovak +axis=swu +czech +ellps=bessel",
    "+proj=krovak +ellps=bessel",
    "+proj=mod_krovak +ellps=bessel +x_0=5000000 +y_0=5000000",
    "+proj=mod_krovak +czech +ellps=bessel",
    "+proj=somerc +lat_0=46.95 +lon_0=7.44 +k_0=1 +x_0=2600000 +y_0=1200000",
    "+proj=nzmg +ellps=intl",
    "+proj=geos +h=35785831 +sweep=x +lon_0=-75",
    "+proj=geos +h=35785863 +lon_0=140.7 +ellps=WGS84",
    "+proj=labrd +lat_0=-18.9 +lon_0=46.43 +azi=18.9 +k=0.9995 +x_0=400000",
];
