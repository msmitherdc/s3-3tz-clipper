#!/bin/bash
set -e

# S3 Source Dataset Details
BUCKET="grid-dev-test-files"
KEY="mesh/fixtures/jacksonville.3tz"

echo "==========================================="
echo "1. Generating Jacksonville, FL GeoJSON Polygon"
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
echo "2. Compiling Rust S3 3tz Clipper"
echo "==========================================="
cargo build --release --target x86_64-unknown-linux-musl

echo "==========================================="
echo "3. Clipping S3 Dataset (s3://$BUCKET/$KEY)"
echo "==========================================="
./target/x86_64-unknown-linux-musl/release/s3-3tz-clipper \
  --bucket "$BUCKET" \
  --key "$KEY" \
  --geojson "jacksonville_clip.geojson" \
  --output "clipped-jacksonville.3tz" \
  --progress \
  --concurrency 30 \
  --no-sign-request # <-- Add this flag for public buckets

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
