use geo::{Intersects, Polygon, Rect, Coord, LineString, BoundingRect};
use geojson::GeoJson;
use serde_json::Value as JsonValue;
use std::collections::{HashSet, VecDeque, HashMap};
use std::path::Path;
use s2::cellid::CellID;
use s2::cell::Cell;
use s2::latlng::LatLng;

// --- I3S Specific Structs ---
// These are constructed manually in main.rs (not via serde), so no Deserialize derives.

#[derive(Debug, Clone)]
pub struct ChildRef {
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct I3SNode {
    pub id: String,
    /// Canonical per-node document path within the archive (without `.gz`),
    /// e.g. "nodes/3/3dNodeIndexDocument.json". Kept so the entry survives clipping
    /// in flat one-node-per-file layouts.
    pub doc_filename: String,
    /// The archive file this node was actually parsed from. For paginated 1.7+
    /// layouts this is a `nodepages/{N}.json[.gz]`; the per-node `doc_filename`
    /// above may or may not exist as a separate archive entry.
    pub containing_doc: String,
    /// Minimum Bounding Sphere: [center_lon, center_lat, center_z, radius_meters].
    /// Derived from `mbs` (1.6) or `obb` (1.7+, approximated as a sphere).
    pub mbs: [f64; 4],
    pub children: Vec<ChildRef>,
}

// --- General Functions ---

pub fn parse_geojson_polygon(geojson_str: &str) -> Option<Polygon<f64>> {
    let geojson = geojson_str.parse::<GeoJson>().ok().or_else(|| {
        serde_json::from_str::<geojson::FeatureCollection>(geojson_str)
            .ok()
            .map(GeoJson::from)
    })?;

    match geojson {
        GeoJson::FeatureCollection(collection) => {
            collection.features.into_iter().find_map(|feature| {
                feature.geometry.and_then(|geometry| {
                    if let geojson::Value::Polygon(poly) = geometry.value {
                        Polygon::try_from(geojson::Value::Polygon(poly)).ok()
                    } else {
                        None
                    }
                })
            })
        }
        GeoJson::Feature(feature) => feature.geometry.and_then(|geometry| {
            if let geojson::Value::Polygon(poly) = geometry.value {
                Polygon::try_from(geojson::Value::Polygon(poly)).ok()
            } else {
                None
            }
        }),
        GeoJson::Geometry(geometry) => {
            if let geojson::Value::Polygon(poly) = geometry.value {
                Polygon::try_from(geojson::Value::Polygon(poly)).ok()
            } else {
                None
            }
        }
    }
}

/// Resolve a URI that may be relative (e.g. "../shared/sharedResource" or "./geometryData/0")
/// against a base path (the directory containing the node document).
/// Returns None if the resolved path would escape the archive root (path traversal).
pub fn resolve_uri(base_doc_path: &str, href: &str) -> Option<String> {
    // Build an absolute-style path by joining base dir + href, then normalize.
    let base_dir = Path::new(base_doc_path).parent().unwrap_or(Path::new(""));
    let joined = base_dir.join(href.trim_start_matches("./"));

    // Normalize by processing components, rejecting upward escapes past root.
    let mut parts: Vec<&str> = Vec::new();
    for component in joined.components() {
        use std::path::Component;
        match component {
            Component::ParentDir => {
                if parts.is_empty() {
                    // Would escape archive root — reject.
                    return None;
                }
                parts.pop();
            }
            Component::Normal(s) => {
                parts.push(s.to_str()?);
            }
            Component::CurDir => {}
            // RootDir / Prefix shouldn't appear since we start from a relative path,
            // but treat them as a hard stop.
            _ => return None,
        }
    }
    Some(parts.join("/"))
}

fn rad_to_deg(rad: f64) -> f64 {
    rad * 180.0 / std::f64::consts::PI
}

// --- I3S Clipping Logic ---

pub fn filter_i3s_scenelayer(
    scenelayer: &JsonValue,
    all_nodes: &HashMap<String, I3SNode>,
    polygon: &Polygon<f64>,
    keep_uris: &mut HashSet<String>,
    kept_node_ids: &mut HashSet<String>,
) {
    // Determine the root node ID from the scenelayer JSON.
    // The store.rootNode value is typically "./nodes/root" or "./nodes/0".
    let root_node_path_str = scenelayer
        .get("store")
        .and_then(|s| s.get("rootNode"))
        .and_then(|r| r.as_str())
        .unwrap_or("./nodes/root");
    let root_id = Path::new(root_node_path_str)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("root")
        .to_string();

    let mut queue: VecDeque<String> = VecDeque::new();

    // For I3S 1.7+ node-page datasets the tree root is always node index "0".
    // The archive also contains a legacy `nodes/root/3dNodeIndexDocument.json` whose
    // children list uses old-style string IDs covering only the top 1-2 levels of the
    // tree — starting traversal there causes BFS to miss the vast majority of nodes.
    // Always prefer "0" (node-page global index); fall back to the store.rootNode
    // string only if "0" is absent (pure 1.6 dataset).
    let root_candidates: &[&str] = &["0", root_id.as_str(), "root"];

    let mut found_root = false;
    for candidate in root_candidates {
        if all_nodes.contains_key(*candidate) {
            queue.push_back(candidate.to_string());
            found_root = true;
            break;
        }
    }

    if !found_root {
        eprintln!("[ERROR] Could not find root node ('0' or '{}') in {} parsed nodes.", root_id, all_nodes.len());
        return;
    }

    let polygon_bbox = match polygon.bounding_rect() {
        Some(rect) => rect,
        None => {
            eprintln!("[ERROR] Clip polygon has no bounding rect.");
            return;
        }
    };

    let mut visited: HashSet<String> = HashSet::new();

    while let Some(node_id) = queue.pop_front() {
        if !visited.insert(node_id.clone()) {
            continue;
        }

        let node = match all_nodes.get(&node_id) {
            Some(n) => n,
            None => {
                eprintln!("[WARN] Node '{}' referenced but not found in parsed node map.", node_id);
                continue;
            }
        };

        // --- Bounding sphere intersection test ---
        // mbs = [center_lon, center_lat, center_z, radius_meters]
        let mbs_center_x = node.mbs[0];
        let mbs_center_y = node.mbs[1];
        let mbs_radius_meters = node.mbs[3];

        // Convert radius from meters to degrees (approximate, good enough for clipping).
        let lat_rad = mbs_center_y.to_radians();
        let meters_per_deg_lat = 111320.0;
        let meters_per_deg_lon = (111320.0 * lat_rad.cos()).max(1.0);

        let radius_deg_x = mbs_radius_meters / meters_per_deg_lon;
        let radius_deg_y = mbs_radius_meters / meters_per_deg_lat;

        let node_bbox = Rect::new(
            Coord {
                x: mbs_center_x - radius_deg_x,
                y: mbs_center_y - radius_deg_y,
            },
            Coord {
                x: mbs_center_x + radius_deg_x,
                y: mbs_center_y + radius_deg_y,
            },
        );

        // Always enqueue children for traversal regardless of whether this node
        // intersects. The I3S LOD tree is NOT a strict spatial containment hierarchy —
        // a parent's MBS does not always tightly bound all its children's bounds, so
        // culling children based on parent intersection causes nodes to be missed.
        // We only gate *resource keeping* on the intersection test.
        for child in &node.children {
            if !visited.contains(&child.id) {
                queue.push_back(child.id.clone());
            }
        }

        // Fast AABB pre-check before the more expensive polygon intersection.
        if !polygon_bbox.intersects(&node_bbox) {
            continue;
        }

        if !polygon.intersects(&node_bbox) {
            continue;
        }

        // This node intersects: keep its document(s). Per-node resources
        // (geometries/, textures/, attributes/, etc.) are expanded by the caller
        // via a prefix scan over the archive entries for each kept node id.
        keep_uris.insert(node.containing_doc.clone());
        keep_uris.insert(node.doc_filename.clone());
        kept_node_ids.insert(node.id.clone());
    }
}

// --- 3D Tiles Clipping Logic ---

pub fn tile_intersects(tile: &JsonValue, polygon: &Polygon<f64>) -> bool {
    let bounding_volume = match tile.get("boundingVolume") {
        Some(bv) => bv,
        None => return false,
    };

    // S2 cell bounding volume (3DTILES_bounding_volume_S2 extension).
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
                        y: latlng.lat.deg(),
                    });
                }
                coords.push(coords[0]);
                return polygon.intersects(&Polygon::new(LineString::from(coords), vec![]));
            }
        }
    }

    // Region bounding volume (radians → degrees).
    if let Some(region) = bounding_volume.get("region").and_then(|r| r.as_array()) {
        if region.len() >= 4 {
            let west = rad_to_deg(region[0].as_f64().unwrap_or(0.0));
            let south = rad_to_deg(region[1].as_f64().unwrap_or(0.0));
            let east = rad_to_deg(region[2].as_f64().unwrap_or(0.0));
            let north = rad_to_deg(region[3].as_f64().unwrap_or(0.0));
            let rect = Rect::new(
                Coord { x: west, y: south },
                Coord { x: east, y: north },
            );
            return polygon.intersects(&rect);
        }
    }

    // Box bounding volume: conservative keep (no easy 2D projection without full OBB math).
    if bounding_volume.get("box").and_then(|b| b.as_array()).is_some() {
        return true;
    }

    // Unknown bounding volume type: conservative keep.
    true
}

fn filter_node(
    node: &mut JsonValue,
    polygon: &Polygon<f64>,
    keep_uris: &mut Vec<String>,
    is_root: bool,
) {
    if !is_root && !tile_intersects(node, polygon) {
        return;
    }
    if let Some(content) = node
        .get("content")
        .and_then(|c| c.get("uri"))
        .and_then(|u| u.as_str())
    {
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
            if tile_intersects(child, polygon) {
                filter_node(child, polygon, keep_uris, false);
                true
            } else {
                false
            }
        });
    }
}

pub fn filter_tileset(
    mut tileset: JsonValue,
    polygon: &Polygon<f64>,
    keep_uris: &mut Vec<String>,
) -> JsonValue {
    if let Some(root) = tileset.get_mut("root") {
        filter_node(root, polygon, keep_uris, true);
    }
    tileset
}