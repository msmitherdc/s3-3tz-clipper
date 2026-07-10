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
    let base_doc_path_norm = base_doc_path.replace('\\', "/");
    let href_norm = href.replace('\\', "/");
    let base_dir = Path::new(&base_doc_path_norm).parent().unwrap_or(Path::new(""));
    let joined = base_dir.join(href_norm.trim_start_matches("./"));

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
    let root_node_path_norm = root_node_path_str.replace('\\', "/");
    let root_id = Path::new(&root_node_path_norm)
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

    // Region bounding volume (radians → degrees). Values are defaulted to 0.0 rather than
    // dropped so a non-numeric entry can't shift the remaining west/south/east/north
    // positions out of alignment.
    if let Some(region) = bounding_volume.get("region").and_then(|r| r.as_array()) {
        let values: Vec<f64> = region.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect();
        if let Some((rect, _, _)) = region_to_rect(&values) {
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
    base_doc_path: &str,
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
        if let Some(resolved) = resolve_uri(base_doc_path, content) {
            keep_uris.push(resolved);
        }
    }
    if let Some(contents) = node.get("contents").and_then(|c| c.as_array()) {
        for content in contents {
            if let Some(uri) = content.get("uri").and_then(|u| u.as_str()) {
                if let Some(resolved) = resolve_uri(base_doc_path, uri) {
                    keep_uris.push(resolved);
                }
            }
        }
    }
    if let Some(children) = node.get_mut("children").and_then(|c| c.as_array_mut()) {
        children.retain_mut(|child| {
            if tile_intersects(child, polygon) {
                filter_node(child, base_doc_path, polygon, keep_uris, false);
                true
            } else {
                false
            }
        });
    }
}

pub fn filter_tileset(
    mut tileset: JsonValue,
    base_doc_path: &str,
    polygon: &Polygon<f64>,
    keep_uris: &mut Vec<String>,
) -> JsonValue {
    if let Some(root) = tileset.get_mut("root") {
        filter_node(root, base_doc_path, polygon, keep_uris, true);
    }
    tileset
}

// --- Package Tileset Clipping Logic ---
//
// A "package" tileset.json (as produced by OWT/Vricon) isn't itself a `.3tz`/`.spk`
// archive - it's a bare JSON file whose root.children each point (via `content.uri`) at an
// entirely separate, independently-indexed archive. filter_tileset/filter_node above only
// prune an archive's own internal children; they never touch the root node's own
// boundingVolume. The functions below let a caller shrink a package's per-child (and
// overall root) `region` after each referenced archive has been clipped independently, so
// the outer tileset's own metadata doesn't keep advertising the pre-clip extent.

fn deg_to_rad(deg: f64) -> f64 {
    deg.to_radians()
}

/// Parse a 3D Tiles `region` bounding volume (`[west, south, east, north, minHeight,
/// maxHeight]`, first four in radians) into a degree-space `Rect` plus its original height
/// range. Only the first four components are required (matching `tile_intersects`'s own
/// tolerance below, which never looks at height) - missing height components default to
/// `0.0`. Returns `None` if `region` doesn't even have west/south/east/north.
pub fn region_to_rect(region: &[f64]) -> Option<(Rect<f64>, f64, f64)> {
    if region.len() < 4 {
        return None;
    }
    let west = rad_to_deg(region[0]);
    let south = rad_to_deg(region[1]);
    let east = rad_to_deg(region[2]);
    let north = rad_to_deg(region[3]);
    let min_height = region.get(4).copied().unwrap_or(0.0);
    let max_height = region.get(5).copied().unwrap_or(0.0);
    Some((
        Rect::new(Coord { x: west, y: south }, Coord { x: east, y: north }),
        min_height,
        max_height,
    ))
}

/// Shrink a `region` bounding volume down to its overlap with the clip polygon's bounding
/// box, keeping the original height range unchanged (a 2D clip polygon says nothing about
/// vertical extent). Returns `None` if the region doesn't intersect the polygon at all -
/// the caller should drop whatever referenced this region (it has nothing left to keep).
pub fn clip_region(region: &[f64], polygon: &Polygon<f64>) -> Option<[f64; 6]> {
    let (rect, min_height, max_height) = region_to_rect(region)?;
    if !polygon.intersects(&rect) {
        return None;
    }
    let polygon_bbox = polygon.bounding_rect()?;
    let west = rect.min().x.max(polygon_bbox.min().x);
    let south = rect.min().y.max(polygon_bbox.min().y);
    let east = rect.max().x.min(polygon_bbox.max().x);
    let north = rect.max().y.min(polygon_bbox.max().y);
    // Guard against a degenerate (edge-touching-only) overlap collapsing to zero area.
    if west >= east || south >= north {
        return None;
    }
    Some([
        deg_to_rad(west),
        deg_to_rad(south),
        deg_to_rad(east),
        deg_to_rad(north),
        min_height,
        max_height,
    ])
}

/// Recompute a whole-tileset `region` as the envelope of its children's own (already
/// clipped) `region`s - used to rewrite a package's own root-level boundingVolume once
/// every referenced archive has been clipped independently.
pub fn union_regions(regions: &[[f64; 6]]) -> Option<[f64; 6]> {
    let mut iter = regions.iter();
    let mut acc = *iter.next()?;
    for r in iter {
        acc[0] = acc[0].min(r[0]);
        acc[1] = acc[1].min(r[1]);
        acc[2] = acc[2].max(r[2]);
        acc[3] = acc[3].max(r[3]);
        acc[4] = acc[4].min(r[4]);
        acc[5] = acc[5].max(r[5]);
    }
    Some(acc)
}