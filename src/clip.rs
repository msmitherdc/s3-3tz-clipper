use geo::{Contains, Intersects, Point, Polygon, Rect};
use geojson::{GeoJson, Value};
use serde_json::Value as JsonValue;

/// Parses a raw GeoJSON string and extracts the first valid Polygon.
pub fn parse_geojson_polygon(geojson_str: &str) -> Result<Polygon<f64>, Box<dyn std::error::Error + Send + Sync>> {
    let geojson = geojson_str.parse::<GeoJson>()?;
    match geojson {
        GeoJson::FeatureCollection(fc) => {
            for feature in fc.features {
                if let Some(geometry) = feature.geometry {
                    if let Value::Polygon(poly_coords) = geometry.value {
                        return Ok(geo::Polygon::new(
                            geo::LineString::from(poly_coords[0].iter().map(|c| (c[0], c[1])).collect::<Vec<_>>()),
                            vec![],
                        ));
                    }
                }
            }
            Err("No Polygon found in FeatureCollection".into())
        }
        GeoJson::Feature(feature) => {
            if let Some(geometry) = feature.geometry {
                if let Value::Polygon(poly_coords) = geometry.value {
                    return Ok(geo::Polygon::new(
                        geo::LineString::from(poly_coords[0].iter().map(|c| (c[0], c[1])).collect::<Vec<_>>()),
                        vec![],
                    ));
                }
            }
            Err("Feature is not a Polygon".into())
        }
        GeoJson::Geometry(geometry) => {
            if let Value::Polygon(poly_coords) = geometry.value {
                return Ok(geo::Polygon::new(
                    geo::LineString::from(poly_coords[0].iter().map(|c| (c[0], c[1])).collect::<Vec<_>>()),
                    vec![],
                ));
            }
            Err("Geometry is not a Polygon".into())
        }
    }
}

// -----------------------------------------------------------------------------
// 3D TILES (.3tz) FILTERING
// -----------------------------------------------------------------------------

/// Recursively traverses a 3D Tiles `tileset.json` structure.
/// If a node intersects the polygon, it is kept. If it points to an external URI,
/// that URI is captured for subsequent fetching.
pub fn filter_tileset(mut tileset: JsonValue, polygon: &Polygon<f64>, local_uris: &mut Vec<String>) -> JsonValue {
    if let Some(root) = tileset.get_mut("root") {
        if !filter_node(root, polygon, local_uris) {
            // If the absolute root does not intersect, the entire dataset is effectively empty for this clip.
            // We clear its children to prevent massive empty structure rendering.
            if let Some(children) = root.get_mut("children") {
                if let Some(arr) = children.as_array_mut() {
                    arr.clear();
                }
            }
        }
    }
    tileset
}

fn filter_node(node: &mut JsonValue, polygon: &Polygon<f64>, local_uris: &mut Vec<String>) -> bool {
    // Determine if this specific node's bounding volume intersects our clip boundary
    let intersects = if let Some(bv) = node.get("boundingVolume") {
        check_bounding_volume(bv, polygon)
    } else {
        true // If no bounding volume is declared, we conservatively keep it.
    };

    if intersects {
        // If it intersects, we need its content (the actual .b3dm, .glb, or external .json)
        if let Some(content) = node.get("content") {
            if let Some(uri) = content.get("uri").or_else(|| content.get("url")).and_then(|u| u.as_str()) {
                local_uris.push(uri.to_string());
            }
        }
        // Support for multiple contents (3D Tiles 1.1)
        if let Some(contents) = node.get("contents").and_then(|c| c.as_array()) {
            for content in contents {
                if let Some(uri) = content.get("uri").or_else(|| content.get("url")).and_then(|u| u.as_str()) {
                    local_uris.push(uri.to_string());
                }
            }
        }
    }

    // Now recursively evaluate children.
    // Even if a parent node intersects, some of its children might be completely outside the clip polygon.
    if let Some(children) = node.get_mut("children").and_then(|c| c.as_array_mut()) {
        let mut kept_children = Vec::new();
        for mut child in children.drain(..) {
            if filter_node(&mut child, polygon, local_uris) {
                kept_children.push(child);
            }
        }
        *children = kept_children;
    }

    intersects
}

fn check_bounding_volume(bv: &JsonValue, polygon: &Polygon<f64>) -> bool {
    // 1. Check "region" (Longitude, Latitude, Height)
    // [west, south, east, north, minHeight, maxHeight]
    if let Some(region) = bv.get("region").and_then(|r| r.as_array()) {
        if region.len() >= 4 {
            // Region values are in radians. Convert to degrees for GeoJSON matching.
            let west = region[0].as_f64().unwrap_or(0.0).to_degrees();
            let south = region[1].as_f64().unwrap_or(0.0).to_degrees();
            let east = region[2].as_f64().unwrap_or(0.0).to_degrees();
            let north = region[3].as_f64().unwrap_or(0.0).to_degrees();

            let rect = Rect::new(
                geo::coord! { x: west, y: south },
                geo::coord! { x: east, y: north },
            );
            return polygon.intersects(&rect) || polygon.contains(&rect) || rect.contains(polygon);
        }
    }

    // 2. Check "sphere" [x, y, z, radius]
    if let Some(sphere) = bv.get("sphere").and_then(|s| s.as_array()) {
        if sphere.len() == 4 {
            // Note: 3D Tiles spheres are usually in EPSG:4978 (ECEF) coordinates.
            // For a perfectly rigorous intersection, we'd project this to WGS84.
            // However, a fast bounding sphere check often simply assumes the center point
            // must fall roughly near the polygon. Here we do a generic fallback to true
            // if we can't easily project it, but in an advanced tool, you would inject PROJ here.
            return true; 
        }
    }

    // 3. Check "box" (OBB - 12 elements)
    if let Some(_box_arr) = bv.get("box").and_then(|b| b.as_array()) {
        // Similar to sphere, OBBs are in ECEF. Without a heavy projection library,
        // we conservatively return true to ensure we don't accidentally clip required data.
        return true;
    }

    true // Default to keeping the node if volume type is unknown
}


// -----------------------------------------------------------------------------
// I3S (.slpk) FILTERING
// -----------------------------------------------------------------------------

/// Traverses an I3S Node Page json structure.
/// I3S uses an array of node objects. We evaluate their "mbs" (Minimum Bounding Sphere)
/// or "obb" against the clip polygon.
pub fn filter_i3s_node(mut node_page: JsonValue, polygon: &Polygon<f64>, local_uris: &mut Vec<String>) -> JsonValue {
    if let Some(nodes) = node_page.get_mut("nodes").and_then(|n| n.as_array_mut()) {
        let mut kept_nodes = Vec::new();

        for mut node in nodes.drain(..) {
            let mut intersects = true;

            // I3S usually uses MBS (Minimum Bounding Sphere) in WGS84 for node evaluation
            // Format: [lon, lat, z, radius_meters]
            if let Some(mbs) = node.get("mbs").and_then(|m| m.as_array()) {
                if mbs.len() == 4 {
                    let lon = mbs[0].as_f64().unwrap_or(0.0);
                    let lat = mbs[1].as_f64().unwrap_or(0.0);
                    // Fast point-in-polygon check for the center of the sphere.
                    // For massive bounding spheres, you'd want a radius distance check.
                    let center = Point::new(lon, lat);
                    
                    // Simple heuristic: Does the polygon intersect the center, or is the center very close?
                    intersects = polygon.contains(&center) || polygon.intersects(&center);
                }
            }

            if intersects {
                // If the node intersects, capture its relative URI pointers to fetch its payload
                // In I3S, resources are often referenced directly by node ID in relative subfolders
                if let Some(node_id) = node.get("id").and_then(|id| id.as_str()) {
                    // Standard I3S payload structures:
                    if node.get("geometryData").is_some() {
                        local_uris.push(format!("geometries/{}.bin", node_id));
                        local_uris.push(format!("geometries/{}.draco", node_id)); // Optional compressed format
                    }
                    if node.get("textureData").is_some() {
                        local_uris.push(format!("textures/{}_0_1.ktx2", node_id));
                        local_uris.push(format!("textures/{}_0_1.jpg", node_id));
                    }
                    if node.get("attributeData").is_some() {
                        local_uris.push(format!("attributes/{}/0.bin", node_id));
                    }
                    if node.get("children").is_some() {
                        // In older I3S, nodes directly referenced child node pages
                        local_uris.push(format!("nodes/{}.json", node_id));
                    }
                }
                
                // If it explicitly declares child node pages
                if let Some(children_pages) = node.get("children").and_then(|c| c.as_array()) {
                     for child in children_pages {
                         if let Some(child_id) = child.as_str() {
                              local_uris.push(format!("nodes/{}.json", child_id));
                         }
                     }
                }

                kept_nodes.push(node);
            }
        }
        *node_page.get_mut("nodes").unwrap() = JsonValue::Array(kept_nodes);
    }

    node_page
}
