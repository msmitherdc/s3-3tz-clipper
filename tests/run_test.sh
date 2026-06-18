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

echo "==========================================="
echo "✅ SUCCESS: Clipped, decompressed, and indexed s3://$BUCKET/$I3S_KEY!"
echo "==========================================="