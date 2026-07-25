#!/bin/bash
set -e

# ==========================================
# TEST CONFIGURATION
# ==========================================
BUCKET="grid-partner-share"
CES_TILES_KEY="mesh/fixtures/jacksonville.3tz"
I3S_KEY="mesh/fixtures/montreal_buildings_v17_21778.slpk"
NO_SIGN_REQUEST="--no-sign-request"

echo "==========================================="
echo "1. Generating Downtown Jacksonville (1/25th Scale) GeoJSON"
echo "==========================================="
cat <<EOF > jacksonville_clip.geojson
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-81.661, 30.319],
            [-81.649, 30.319],
            [-81.649, 30.331],
            [-81.661, 30.331],
            [-81.661, 30.319]
          ]
        ]
      }
    }
  ]
}
EOF


cat <<EOF > montreal_clip.geojson
{"type":"Feature","properties":{"PROJECT_NAME":"ArronMontreal"},"geometry":{
"type":"Polygon","coordinates":[[
[-73.561499734903904,45.508849820695801],
[-73.555634056266896,45.506115268239505],
[-73.553813820305194,45.508379315242301],
[-73.559928850229099,45.510789385432794],
[-73.561499734903904,45.508849820695801]
]]
}
}
EOF

# Assert the zip listing of $1 contains a path matching regex $2 ($3 = human label).
assert_zip_contains() {
    local archive="$1" pattern="$2" label="$3"
    if ! unzip -l "$archive" | grep -Eq "$pattern"; then
        echo "❌ ERROR: $archive is missing $label (no entry matching '$pattern')"
        unzip -l "$archive" | head -n 40
        exit 1
    fi
    echo "✔ $archive contains $label"
}

echo "==========================================="
echo "2. Locating CI-Compiled Binary"
echo "==========================================="
# Locate the binary compiled by the CI runner
if [ -f "./target/release/s3-3tz-clipper" ]; then
    BINARY="./target/release/s3-3tz-clipper"
elif [ -f "./target/aarch64-apple-darwin/release/s3-3tz-clipper" ]; then
    BINARY="./target/aarch64-apple-darwin/release/s3-3tz-clipper"
else
    echo "❌ ERROR: No compiled binary found in target/release/ or target/aarch64-apple-darwin/release/"
    exit 1
fi

echo "Using pre-compiled binary located at: $BINARY"
echo "==========================================="
echo "3. Clipping S3 Dataset (s3://$BUCKET/$CES_TILES_KEY)"
echo "==========================================="
$BINARY \
  --bucket "$BUCKET" \
  --key "$CES_TILES_KEY" \
  --geojson "jacksonville_clip.geojson" \
  --output "clipped-jacksonville.3tz" \
  --progress \
  --concurrency 10 \
  $NO_SIGN_REQUEST

echo "==========================================="
echo "4. Validating 3DTiles Output File Structure"
echo "==========================================="
if [ ! -f "clipped-jacksonville.3tz" ]; then
    echo "❌ ERROR: Output file clipped-jacksonville.3tz was not created!"
    exit 1
fi

unzip -l clipped-jacksonville.3tz | head -n 25

# A usable clipped .3tz must have the root tileset, the offset index, and actual
# tile content payloads - not just JSON metadata.
assert_zip_contains clipped-jacksonville.3tz 'tileset\.json(\.gz)?$' "root tileset.json"
assert_zip_contains clipped-jacksonville.3tz '@3dtilesIndex1@' "the @3dtilesIndex1@ offset index"
assert_zip_contains clipped-jacksonville.3tz '\.(b3dm|glb|i3dm|pnts|cmpt)(\.gz)?$' "tile content payloads"

echo "==========================================="
echo "✅ SUCCESS: Clipped, decompressed, and indexed s3://$BUCKET/$CES_TILES_KEY!"
echo "==========================================="

echo "Using pre-compiled binary located at: $BINARY"
echo "==========================================="
echo "3. Clipping I3S S3 Dataset (s3://$BUCKET/$I3S_KEY)"
echo "==========================================="
$BINARY \
  --bucket "$BUCKET" \
  --key "$I3S_KEY" \
  --geojson "montreal_clip.geojson" \
  --output "clipped-montreal.spk" \
  --progress \
  --concurrency 10 \
  $NO_SIGN_REQUEST

echo "==========================================="
echo "4. Validating Output File Structure"
echo "==========================================="
if [ ! -f "clipped-montreal.spk" ]; then
    echo "❌ ERROR: Output file clipped-montreal.spk was not created!"
    exit 1
fi

unzip -l clipped-montreal.spk | head -n 25

# A usable clipped SLPK must have the scene layer, the offset index, node documents,
# and - critically - the per-node payloads (geometries/textures/...). A regression
# once shipped output with only node metadata and zero renderable content; the
# geometries assertion is what catches that class of bug.
assert_zip_contains clipped-montreal.spk '3dSceneLayer\.json(\.gz)?$' "3dSceneLayer.json"
assert_zip_contains clipped-montreal.spk '@specialIndexFileHASH128@' "the @specialIndexFileHASH128@ offset index"
assert_zip_contains clipped-montreal.spk 'nodes/[^/]+/' "per-node entries"
assert_zip_contains clipped-montreal.spk 'geometries/' "node geometry payloads"

echo "==========================================="
echo "✅ SUCCESS: Clipped, decompressed, and indexed s3://$BUCKET/$I3S_KEY!"
echo "==========================================="

echo "==========================================="
echo "5. Clipping the same 3DTiles dataset from a LOCAL path (no --bucket)"
echo "==========================================="
# Stage the fixture on disk, then clip it with --bucket omitted. The local path must produce
# the same set of entries as the S3 run above - it is the same pipeline with positional reads
# swapped in for HTTP range requests.
curl -sS -o "local-jacksonville.3tz" \
  "https://$BUCKET.s3.amazonaws.com/$CES_TILES_KEY"

$BINARY \
  --key "local-jacksonville.3tz" \
  --geojson "jacksonville_clip.geojson" \
  --output "clipped-jacksonville-local.3tz" \
  --progress \
  --concurrency 10

if [ ! -f "clipped-jacksonville-local.3tz" ]; then
    echo "❌ ERROR: Output file clipped-jacksonville-local.3tz was not created!"
    exit 1
fi

assert_zip_contains clipped-jacksonville-local.3tz 'tileset\.json(\.gz)?$' "root tileset.json"
assert_zip_contains clipped-jacksonville-local.3tz '@3dtilesIndex1@' "the @3dtilesIndex1@ offset index"
assert_zip_contains clipped-jacksonville-local.3tz '\.(b3dm|glb|i3dm|pnts|cmpt)(\.gz)?$' "tile content payloads"

# The local and S3 runs must agree on entry names (byte order within the archive can differ,
# since entries are written in fetch-completion order).
if ! diff <(unzip -l clipped-jacksonville.3tz | awk '{print $4}' | sort) \
          <(unzip -l clipped-jacksonville-local.3tz | awk '{print $4}' | sort) > /dev/null; then
    echo "❌ ERROR: local-path output does not contain the same entries as the S3 output"
    exit 1
fi
echo "✔ local-path output matches the S3 output entry-for-entry"

echo "==========================================="
echo "✅ SUCCESS: Clipped and indexed a local .3tz with no bucket!"
echo "==========================================="

echo "==========================================="
echo "6. I3S 1.6 traversal must descend through non-intersecting ancestors"
echo "==========================================="
# The I3S LOD tree is NOT a strict spatial containment hierarchy: a parent's MBS does not
# always bound its children's. This fixture puts the root and an intermediate node far outside
# the clip polygon with a leaf *inside* it - culling the subtree at the first non-intersecting
# node yields an archive with zero renderable content.
python3 - <<'PYEOF'
import zipfile, json
def doc(nid, lon, lat, r, kids):
    return json.dumps({"id": nid, "level": 0, "mbs": [lon, lat, 0.0, r],
                       "children": [{"id": k, "href": f"../{k}"} for k in kids]})
with zipfile.ZipFile("i3s16_fixture.slpk", "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("3dSceneLayer.json", json.dumps({
        "id": 0, "version": "1.6", "name": "synthetic",
        "store": {"id": "s", "profile": "meshpyramids", "rootNode": "./nodes/root", "version": "1.6"}}))
    z.writestr("nodes/root/3dNodeIndexDocument.json", doc("root", 50.0, 50.0, 10.0, ["mid"]))
    z.writestr("nodes/mid/3dNodeIndexDocument.json",  doc("mid",  40.0, 40.0, 10.0, ["leaf"]))
    z.writestr("nodes/leaf/3dNodeIndexDocument.json", doc("leaf",  0.5,  0.5, 50.0, []))
    z.writestr("nodes/leaf/geometries/0.bin", b"LEAF-GEOMETRY-PAYLOAD" * 50)
    z.writestr("nodes/leaf/textures/0_0.jpg", b"LEAF-TEXTURE-PAYLOAD" * 50)
json.dump({"type": "Feature", "properties": {}, "geometry": {"type": "Polygon",
    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}}, open("unit_clip.geojson", "w"))
PYEOF

$BINARY \
  --key "i3s16_fixture.slpk" \
  --geojson "unit_clip.geojson" \
  --output "clipped-i3s16.spk"

assert_zip_contains clipped-i3s16.spk 'nodes/leaf/3dNodeIndexDocument\.json' "the in-polygon leaf node reached through two out-of-polygon ancestors"
assert_zip_contains clipped-i3s16.spk 'nodes/leaf/geometries/0\.bin' "the leaf's geometry payload"
assert_zip_contains clipped-i3s16.spk 'nodes/leaf/textures/0_0\.jpg' "the leaf's texture payload"

# The out-of-polygon ancestors themselves must still be dropped - descending is not keeping.
if unzip -l clipped-i3s16.spk | grep -Eq 'nodes/(root|mid)/'; then
    echo "❌ ERROR: non-intersecting ancestor nodes were kept, not just traversed"
    unzip -l clipped-i3s16.spk
    exit 1
fi
echo "✔ non-intersecting ancestors were traversed but not kept"

echo "==========================================="
echo "✅ SUCCESS: I3S 1.6 traversal descends without over-keeping!"
echo "==========================================="

echo "==========================================="
echo "7. ZIP64: a single entry larger than 4 GiB"
echo "==========================================="
# Opt-in: this holds the whole decompressed entry in memory (~4.5 GiB RSS). Everything else in
# this script runs in a few hundred MB, so it is off by default.
#
# The `zip` crate does NOT upgrade an entry to ZIP64 automatically - writing past
# ZIP64_BYTES_THR (u32::MAX) with `large_file` unset aborts with "Large file option has not
# been set". The payload is all zeros, so a 4 GiB entry costs ~19 MB of fixture on disk and
# ~130 KB of output, but still crosses the threshold (which keys off the *uncompressed* size).
if [ "${RUN_ZIP64_TEST:-0}" != "1" ]; then
    echo "⏭  SKIPPED (set RUN_ZIP64_TEST=1 to run; needs ~4.5 GiB RAM)"
else
    python3 - <<'PYEOF'
import zipfile, json, math
r = lambda d: d * math.pi / 180
TARGET = 4 * 1024**3 + 8 * 1024**2      # strictly over u32::MAX
CHUNK  = b"\0" * (8 * 1024 * 1024)
with zipfile.ZipFile("zip64_fixture.3tz", "w", zipfile.ZIP_DEFLATED, compresslevel=1, allowZip64=True) as z:
    z.writestr("tileset.json", json.dumps({"asset": {"version": "1.0"}, "root": {
        "boundingVolume": {"region": [r(0), r(0), r(1), r(1), 0, 10]},
        "geometricError": 1, "refine": "ADD",
        "children": [{"boundingVolume": {"region": [r(.2), r(.2), r(.8), r(.8), 0, 10]},
                      "geometricError": 0, "content": {"uri": "huge.b3dm"}}]}}))
    # force_zip64: the source entry's uncompressed size is itself over 4 GiB, so this also
    # exercises ZIP64 extra-field parsing on the *read* side.
    with z.open("huge.b3dm", "w", force_zip64=True) as fh:
        written = 0
        while written < TARGET:
            n = min(len(CHUNK), TARGET - written)
            fh.write(CHUNK[:n]); written += n
json.dump({"type": "Feature", "properties": {}, "geometry": {"type": "Polygon",
    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}}, open("unit_clip.geojson", "w"))
PYEOF

    $BINARY \
      --key "zip64_fixture.3tz" \
      --geojson "unit_clip.geojson" \
      --output "clipped-zip64.3tz" \
      --max-entry-size 5120 \
      --concurrency 2

    python3 - <<'PYEOF'
import zipfile, struct, sys, zlib
EXPECT = 4 * 1024**3 + 8 * 1024**2
z = zipfile.ZipFile("clipped-zip64.3tz")
h = z.getinfo("huge.b3dm")
if h.file_size != EXPECT:
    sys.exit(f"❌ ERROR: entry is {h.file_size} bytes, expected {EXPECT}")
# The Local File Header must carry a ZIP64 extra field (tag 0x0001).
with open("clipped-zip64.3tz", "rb") as f:
    f.seek(h.header_offset); lfh = f.read(30)
    nlen, elen = struct.unpack_from("<HH", lfh, 26)
    f.seek(h.header_offset + 30 + nlen); extra = f.read(elen)
tags, o = [], 0
while o + 4 <= len(extra):
    t, sz = struct.unpack_from("<HH", extra, o); tags.append(t); o += 4 + sz
if 0x0001 not in tags:
    sys.exit(f"❌ ERROR: no ZIP64 extra field in the local file header (tags={tags})")
# Stream the payload back: right length, right content, CRC agrees with the directory.
n, crc, bad = 0, 0, 0
with z.open("huge.b3dm") as fh:
    while True:
        b = fh.read(16 * 1024 * 1024)
        if not b: break
        n += len(b); crc = zlib.crc32(b, crc)
        if b.count(0) != len(b): bad += 1
if n != EXPECT or bad or crc != h.CRC:
    sys.exit(f"❌ ERROR: payload readback failed (len={n} bad_chunks={bad} crc_ok={crc == h.CRC})")
print(f"✔ {EXPECT:,}-byte entry round-tripped with ZIP64 headers and a matching CRC")
PYEOF

    assert_zip_contains clipped-zip64.3tz '@3dtilesIndex1@' "the @3dtilesIndex1@ offset index"
    rm -f zip64_fixture.3tz clipped-zip64.3tz
    echo "==========================================="
    echo "✅ SUCCESS: >4 GiB entry written with per-entry ZIP64!"
    echo "==========================================="
fi