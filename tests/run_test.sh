#!/bin/bash
set -e

# S3 Source Dataset Details (Public Partner Share)
BUCKET="grid-partner-share"
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
            [-81.68, 30.30],
            [-81.62, 30.30],
            [-81.62, 30.36],
            [-81.68, 30.36],
            [-81.68, 30.30]
          ]
        ]
      }
    }
  ]
}
EOF

echo "==========================================="
echo "2. Clipping S3 Dataset (s3://$BUCKET/$KEY)"
echo "==========================================="
# Execute the native binary (works on macOS locally, or Linux in CI)
./target/release/s3-3tz-clipper \
  --bucket "$BUCKET" \
  --key "$KEY" \
  --geojson "jacksonville_clip.geojson" \
  --output "clipped-jacksonville.3tz" \
  --progress \
  --concurrency 30

echo "==========================================="
echo "3. Validating Output File Structure"
echo "==========================================="
if [ ! -f "clipped-jacksonville.3tz" ]; then
    echo "❌ ERROR: Output file clipped-jacksonville.3tz was not created!"
    exit 1
fi

# Look for tileset.json and at least one model file
unzip -l clipped-jacksonville.3tz | grep "tileset.json"
unzip -l clipped-jacksonville.3tz | grep -m 1 ".b3dm\|.glb"

echo "==========================================="
echo "✅ SUCCESS: Clipped, decompressed, and indexed s3://$BUCKET/$KEY!"
echo "==========================================="
