#!/bin/bash
set -e

# ==========================================
# TEST CONFIGURATION
# ==========================================
BUCKET="grid-partner-share"
KEY="mesh/fixtures/jacksonville.3tz"
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
echo "3. Clipping S3 Dataset (s3://$BUCKET/$KEY)"
echo "==========================================="
$BINARY \
  --bucket "$BUCKET" \
  --key "$KEY" \
  --geojson "jacksonville_clip.geojson" \
  --output "clipped-jacksonville.3tz" \
  --progress \
  --concurrency 10 \
  --debug \
  $NO_SIGN_REQUEST

echo "==========================================="
echo "4. Validating Output File Structure"
echo "==========================================="
if [ ! -f "clipped-jacksonville.3tz" ]; then
    echo "❌ ERROR: Output file clipped-jacksonville.3tz was not created!"
    exit 1
fi

unzip -l clipped-jacksonville.3tz | head -n 25

echo "==========================================="
echo "✅ SUCCESS: Clipped, decompressed, and indexed s3://$BUCKET/$KEY!"
echo "==========================================="

