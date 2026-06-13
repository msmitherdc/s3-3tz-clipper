use geo::{Intersects, Polygon, Rect, Coord, LineString};
use geojson::{GeoJson, Value};
use serde_json::Value as JsonValue;
use s2::cellid::CellID;
use s2::cell::Cell;
use s2::latlng::LatLng;

/// Parses a GeoJSON polygon into a standard `geo::Polygon`
pub fn parse_geojson_polygon(geojson_str: &str) -> Option<Polygon<f64>> {
    let geojson = geojson_str.parse::<GeoJson>().ok()?;
    
    match geojson {
        // Handle FeatureCollections
        GeoJson::FeatureCollection(collection) => {
            for feature in collection.features {
                if let Some(geometry) = feature.geometry {
                    if let Value::Polygon(poly) = geometry.value {
                        return Polygon::try_from(Value::Polygon(poly)).ok();
                    }
                }
            }
            None
        }
        // Handle a solitary Feature
        GeoJson::Feature(feature) => {
            if let Some(geometry) = feature.geometry {
                if let Value::Polygon(poly) = geometry.value {
                    return Polygon::try_from(Value::Polygon(poly)).ok();
                }
            }
            None
        }
        // Handle a raw Geometry object
        GeoJson::Geometry(geometry) => {
            if let Value::Polygon(poly) = geometry.value {
                return Polygon::try_from(Value::Polygon(poly)).ok();
            }
            None
        }
    }
}

fn rad_to_deg(rad: f64) -> f64 {
    rad * 180.0 / std::f64::consts::PI
}

pub fn tile_intersects(tile: &JsonValue, polygon: &Polygon<f64>) -> bool {
    let bounding_volume = match tile.get("boundingVolume") {
        Some(bv) => bv,
        None => return false,
    };

    // handle S2 Cell Bounding Volume
    if let Some(extensions) = bounding_volume.get("extensions") {
        if let Some(s2_ext) = extensions.get("3DTILES_bounding_volume_S2") {
            if let Some(token) = s2_ext.get("token").and_then(|t| t.as_str()) {
                let cell_id = CellID::from_token(token);
                let cell = Cell::from(cell_id);

                let mut coords = Vec::with_capacity(5);
                for i in 0..4 {
                    let vertex = cell.vertex(i);
                    let latlng = LatLng::from(vertex);
                    coords.push(Coord {
                        x: latlng.lng.deg(),
                        y: latlng.lat.deg()
                    });
                }
                coords.push(coords[0]);

                let s2_poly = Polygon::new(LineString::new(coords), vec![]);
                return polygon.intersects(&s2_poly);
            }
        }
    }

    // CBounding Volume is a Region
    if let Some(region) = bounding_volume.get("region").and_then(|r| r.as_array()) {
        if region.len() >= 4 {
            let west = rad_to_deg(region[0].as_f64().unwrap_or(0.0));
            let south = rad_to_deg(region[1].as_f64().unwrap_or(0.0));
            let east = rad_to_deg(region[2].as_f64().unwrap_or(0.0));
            let north = rad_to_deg(region[3].as_f64().unwrap_or(0.0));

            let rect = Rect::new(
                Coord { x: west, y: south },
                Coord { x: east, y: north }
            );
            return polygon.intersects(&rect);
        }
    }

    // Box Fallback
    if bounding_volume.get("box").and_then(|b| b.as_array()).is_some() {
        return true;
    }

    true
}

pub fn filter_tileset(mut tileset: JsonValue, polygon: &Polygon<f64>, keep_uris: &mut Vec<String>) -> JsonValue {
    if let Some(root) = tileset.get_mut("root") {
        filter_node(root, polygon, keep_uris, true);
    }
    tileset
}

fn filter_node(node: &mut JsonValue, polygon: &Polygon<f64>, keep_uris: &mut Vec<String>, is_root: bool) {
    let intersects = tile_intersects(node, polygon);

    if intersects || is_root {
        if let Some(content) = node.get("content").and_then(|c| c.get("uri")).and_then(|u| u.as_str()) {
            keep_uris.push(content.to_string());
        }
        if let Some(contents) = node.get("contents").and_then(|c| c.as_array()) {
            for content in contents {
                if let Some(uri) = content.get("uri").and_then(|u| u.as_str()) {
                    keep_uris.push(uri.to_string());
                }
            }
        }

        if let Some(children) = node.get_mut("children").and_then(|c| c.as_array_mut()) {
            children.retain_mut(|child| {
                let keep_child = tile_intersects(child, polygon);
                if keep_child {
                    filter_node(child, polygon, keep_uris, false);
                }
                keep_child
            });
        }
    }
}
