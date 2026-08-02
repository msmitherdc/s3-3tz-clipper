use geo::{BoundingRect, Coord, Intersects, LineString, Polygon, Rect};
use geojson::GeoJson;
use s2::cell::Cell;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

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

    // TryFrom errors (and thus yields None) for any non-Polygon geometry.
    fn polygon_of(geometry: &geojson::Geometry) -> Option<Polygon<f64>> {
        Polygon::try_from(&geometry.value).ok()
    }

    match geojson {
        GeoJson::FeatureCollection(collection) => collection
            .features
            .into_iter()
            .find_map(|feature| feature.geometry.as_ref().and_then(polygon_of)),
        GeoJson::Feature(feature) => feature.geometry.as_ref().and_then(polygon_of),
        GeoJson::Geometry(geometry) => polygon_of(&geometry),
    }
}

/// Resolve a URI that may be relative (e.g. "../shared/sharedResource" or "./geometryData/0")
/// against a base path (the directory containing the node document).
/// Returns None if the resolved path would escape the archive root (path traversal).
pub fn resolve_uri(base_doc_path: &str, href: &str) -> Option<String> {
    let base_doc_path_norm = base_doc_path.replace('\\', "/");
    let href_norm = href.replace('\\', "/");
    let base_dir = Path::new(&base_doc_path_norm)
        .parent()
        .unwrap_or(Path::new(""));
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

// --- I3S node parsing (shared by the 1.6 lazy walk and the 1.7+ node-page walk) ---

/// Extract a node's bounds as `[center_lon, center_lat, center_z, radius_meters]`.
///
/// I3S 1.6 nodes carry an `mbs` array directly; 1.7+ nodes carry an `obb` (as either an
/// object with `center`/`halfSize`, or a flat array), which is approximated as the sphere
/// enclosing the box. Returns `None` when the node declares no usable bounds - callers must
/// still traverse such a node's children, they just can't decide whether to keep it.
pub fn parse_node_bounds(node: &JsonValue) -> Option<[f64; 4]> {
    let mut mbs = [0.0f64; 4];

    if let Some(mbs_arr) = node.get("mbs").and_then(|m| m.as_array()) {
        if mbs_arr.len() >= 4 {
            for (i, slot) in mbs.iter_mut().enumerate() {
                *slot = mbs_arr[i].as_f64().unwrap_or(0.0);
            }
            return Some(mbs);
        }
        return None;
    }

    // `obb` in object form: { center: [x,y,z], halfSize: [hx,hy,hz], ... }
    if let Some(obb) = node.get("obb").and_then(|o| o.as_object()) {
        let center = obb.get("center").and_then(|c| c.as_array())?;
        let half_size = obb.get("halfSize").and_then(|h| h.as_array())?;
        if center.len() < 3 || half_size.len() < 3 {
            return None;
        }
        for (i, slot) in mbs.iter_mut().take(3).enumerate() {
            *slot = center[i].as_f64().unwrap_or(0.0);
        }
        let (hx, hy, hz) = (
            half_size[0].as_f64().unwrap_or(0.0),
            half_size[1].as_f64().unwrap_or(0.0),
            half_size[2].as_f64().unwrap_or(0.0),
        );
        mbs[3] = (hx * hx + hy * hy + hz * hz).sqrt();
        return Some(mbs);
    }

    // `obb` in flat-array form: [cx, cy, cz, hx, hy, hz, ...]
    if let Some(obb) = node.get("obb").and_then(|o| o.as_array()) {
        if obb.len() < 6 {
            return None;
        }
        for (i, slot) in mbs.iter_mut().take(3).enumerate() {
            *slot = obb[i].as_f64().unwrap_or(0.0);
        }
        let (hx, hy, hz) = (
            obb[3].as_f64().unwrap_or(0.0),
            obb[4].as_f64().unwrap_or(0.0),
            obb[5].as_f64().unwrap_or(0.0),
        );
        mbs[3] = (hx * hx + hy * hy + hz * hz).sqrt();
        return Some(mbs);
    }

    None
}

/// Project a node's minimum bounding sphere into a degree-space AABB. The metres-per-degree
/// conversion is approximate, which is fine: this only ever widens/narrows a bbox used for a
/// conservative intersection test.
pub fn mbs_to_rect(mbs: &[f64; 4]) -> Rect<f64> {
    let (center_x, center_y, radius_m) = (mbs[0], mbs[1], mbs[3]);
    let meters_per_deg_lat = 111320.0;
    let meters_per_deg_lon = (111320.0 * center_y.to_radians().cos()).max(1.0);
    let radius_deg_x = radius_m / meters_per_deg_lon;
    let radius_deg_y = radius_m / meters_per_deg_lat;

    Rect::new(
        Coord {
            x: center_x - radius_deg_x,
            y: center_y - radius_deg_y,
        },
        Coord {
            x: center_x + radius_deg_x,
            y: center_y + radius_deg_y,
        },
    )
}

/// Read a child reference's node id. Children appear as a bare number, a bare string, or an
/// object carrying `id`/`index`, depending on I3S version and producer.
pub fn child_id_of(child: &JsonValue) -> Option<String> {
    match child {
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Object(o) => match o.get("id").or_else(|| o.get("index"))? {
            JsonValue::String(s) => Some(s.clone()),
            JsonValue::Number(n) => Some(n.to_string()),
            _ => None,
        },
        _ => None,
    }
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
        eprintln!(
            "[ERROR] Could not find root node ('0' or '{}') in {} parsed nodes.",
            root_id,
            all_nodes.len()
        );
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
                eprintln!(
                    "[WARN] Node '{}' referenced but not found in parsed node map.",
                    node_id
                );
                continue;
            }
        };

        // --- Bounding sphere intersection test ---
        let node_bbox = mbs_to_rect(&node.mbs);

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

/// Expand an I3S keep set with every archive entry belonging to a kept node.
///
/// Node documents / node-page entries only carry compact references; the actual payload
/// files (geometries/, textures/, attributes/, features/, shared/, …) live under
/// `nodes/{id}/...`. After traversal, every entry under a kept node's directory must be
/// retained or the clipped archive has no renderable content.
///
/// `nodepages/*` and `statistics/*` entries are kept unconditionally — they are small,
/// densely interlinked, and referenced by integer index from the scene layer, so partial
/// pruning would break the node-page lookup tables. Root-level `metadata.json` (required
/// by several SLPK readers) is kept too when present.
///
/// `entry_names` should be the archive entry names *without* any `.gz` suffix. Returns the
/// number of names newly added to `keep_uris`.
pub fn expand_i3s_keep_set<'a, I>(
    entry_names: I,
    kept_node_ids: &HashSet<String>,
    keep_uris: &mut HashSet<String>,
) -> usize
where
    I: IntoIterator<Item = &'a str>,
{
    // Extract the node id from a path shaped like "[prefix/]nodes/{id}/...".
    fn node_id_of(name: &str) -> Option<&str> {
        let idx = if let Some(rest) = name.strip_prefix("nodes/") {
            return rest.split('/').next().filter(|s| !s.is_empty());
        } else {
            name.find("/nodes/")? + "/nodes/".len()
        };
        name[idx..].split('/').next().filter(|s| !s.is_empty())
    }

    let mut added = 0usize;
    for name in entry_names {
        let is_kept_node_resource = node_id_of(name).is_some_and(|id| kept_node_ids.contains(id));
        let is_nodepage = name.starts_with("nodepages/") || name.contains("/nodepages/");
        let is_statistics = name.starts_with("statistics/") || name.contains("/statistics/");
        let is_root_metadata = name == "metadata.json";

        if (is_kept_node_resource || is_nodepage || is_statistics || is_root_metadata)
            && keep_uris.insert(name.to_string())
        {
            added += 1;
        }
    }
    added
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
    if bounding_volume
        .get("box")
        .and_then(|b| b.as_array())
        .is_some()
    {
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
    // Pre-1.0 tilesets (asset.version "0.0") use `content.url`; 1.0+ uses `content.uri`.
    if let Some(content) = node
        .get("content")
        .and_then(|c| c.get("uri").or_else(|| c.get("url")))
        .and_then(|u| u.as_str())
    {
        if let Some(resolved) = resolve_uri(base_doc_path, content) {
            keep_uris.push(resolved);
        }
    }
    if let Some(contents) = node.get("contents").and_then(|c| c.as_array()) {
        for content in contents {
            if let Some(uri) = content
                .get("uri")
                .or_else(|| content.get("url"))
                .and_then(|u| u.as_str())
            {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Unit square polygon from (0,0) to (1,1) in degrees.
    fn unit_polygon() -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            vec![],
        )
    }

    // --- parse_geojson_polygon ---

    #[test]
    fn parses_feature_collection_feature_and_geometry() {
        let poly = r#"{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}"#;
        let feature = format!(r#"{{"type":"Feature","properties":{{}},"geometry":{poly}}}"#);
        let collection = format!(r#"{{"type":"FeatureCollection","features":[{feature}]}}"#);
        for input in [poly.to_string(), feature, collection] {
            let parsed = parse_geojson_polygon(&input);
            assert!(parsed.is_some(), "failed to parse: {input}");
        }
    }

    #[test]
    fn rejects_invalid_and_non_polygon_geojson() {
        assert!(parse_geojson_polygon("not json").is_none());
        assert!(parse_geojson_polygon(r#"{"type":"Point","coordinates":[0,0]}"#).is_none());
    }

    // --- resolve_uri ---

    #[test]
    fn resolve_uri_relative_forms() {
        assert_eq!(
            resolve_uri("tileset.json", "tiles/0.b3dm").unwrap(),
            "tiles/0.b3dm"
        );
        assert_eq!(
            resolve_uri("sub/tileset.json", "./0.b3dm").unwrap(),
            "sub/0.b3dm"
        );
        assert_eq!(
            resolve_uri("nodes/1/doc.json", "../2/geometry.bin").unwrap(),
            "nodes/2/geometry.bin"
        );
        assert_eq!(resolve_uri("a\\b\\doc.json", "c.bin").unwrap(), "a/b/c.bin");
    }

    #[test]
    fn resolve_uri_rejects_escapes() {
        assert!(resolve_uri("tileset.json", "../outside.b3dm").is_none());
        assert!(resolve_uri("sub/doc.json", "../../outside.b3dm").is_none());
        assert!(resolve_uri("", "../escape").is_none());
    }

    // --- region_to_rect / clip_region / union_regions ---

    #[test]
    fn region_to_rect_requires_four_components() {
        assert!(region_to_rect(&[0.0, 0.0, 0.1]).is_none());
        let (rect, min_h, max_h) = region_to_rect(&[0.0, 0.0, 0.01, 0.01]).unwrap();
        assert!(rect.max().x > 0.0);
        assert_eq!((min_h, max_h), (0.0, 0.0));
        let (_, min_h, max_h) = region_to_rect(&[0.0, 0.0, 0.01, 0.01, -5.0, 120.0]).unwrap();
        assert_eq!((min_h, max_h), (-5.0, 120.0));
    }

    #[test]
    fn clip_region_shrinks_and_preserves_heights() {
        // Region spanning (-1,-1)..(2,2) degrees, clipped by the unit square.
        let region = [
            deg_to_rad(-1.0),
            deg_to_rad(-1.0),
            deg_to_rad(2.0),
            deg_to_rad(2.0),
            -10.0,
            500.0,
        ];
        let clipped = clip_region(&region, &unit_polygon()).unwrap();
        assert!((rad_to_deg(clipped[0]) - 0.0).abs() < 1e-9);
        assert!((rad_to_deg(clipped[1]) - 0.0).abs() < 1e-9);
        assert!((rad_to_deg(clipped[2]) - 1.0).abs() < 1e-9);
        assert!((rad_to_deg(clipped[3]) - 1.0).abs() < 1e-9);
        assert_eq!(clipped[4], -10.0);
        assert_eq!(clipped[5], 500.0);
    }

    #[test]
    fn clip_region_disjoint_or_degenerate_returns_none() {
        let far_away = [
            deg_to_rad(10.0),
            deg_to_rad(10.0),
            deg_to_rad(11.0),
            deg_to_rad(11.0),
            0.0,
            0.0,
        ];
        assert!(clip_region(&far_away, &unit_polygon()).is_none());
        // Shares only the edge x=1 with the unit square -> zero-area overlap.
        let touching = [
            deg_to_rad(1.0),
            deg_to_rad(0.0),
            deg_to_rad(2.0),
            deg_to_rad(1.0),
            0.0,
            0.0,
        ];
        assert!(clip_region(&touching, &unit_polygon()).is_none());
    }

    #[test]
    fn union_regions_envelopes() {
        assert!(union_regions(&[]).is_none());
        let a = [0.0, 0.0, 1.0, 1.0, -5.0, 10.0];
        let b = [-1.0, 0.5, 0.5, 2.0, 0.0, 50.0];
        assert_eq!(
            union_regions(&[a, b]).unwrap(),
            [-1.0, 0.0, 1.0, 2.0, -5.0, 50.0]
        );
    }

    // --- tile_intersects ---

    #[test]
    fn tile_intersects_region_in_and_out() {
        let inside = json!({"boundingVolume": {"region": [
            deg_to_rad(0.25), deg_to_rad(0.25), deg_to_rad(0.75), deg_to_rad(0.75), 0.0, 100.0
        ]}});
        let outside = json!({"boundingVolume": {"region": [
            deg_to_rad(5.0), deg_to_rad(5.0), deg_to_rad(6.0), deg_to_rad(6.0), 0.0, 100.0
        ]}});
        assert!(tile_intersects(&inside, &unit_polygon()));
        assert!(!tile_intersects(&outside, &unit_polygon()));
    }

    #[test]
    fn tile_intersects_conservative_cases() {
        // box volumes and unknown volume types are kept conservatively
        assert!(tile_intersects(
            &json!({"boundingVolume": {"box": vec![0.0; 12]}}),
            &unit_polygon()
        ));
        assert!(tile_intersects(
            &json!({"boundingVolume": {"sphere": [0, 0, 0, 1]}}),
            &unit_polygon()
        ));
        // no boundingVolume at all -> not kept
        assert!(!tile_intersects(
            &json!({"content": {"uri": "x.b3dm"}}),
            &unit_polygon()
        ));
    }

    // --- filter_tileset ---

    fn region_deg(w: f64, s: f64, e: f64, n: f64) -> serde_json::Value {
        json!([
            deg_to_rad(w),
            deg_to_rad(s),
            deg_to_rad(e),
            deg_to_rad(n),
            0.0,
            10.0
        ])
    }

    #[test]
    fn filter_tileset_prunes_children_and_collects_uris() {
        let tileset = json!({
            "root": {
                "boundingVolume": {"region": region_deg(-2.0, -2.0, 3.0, 3.0)},
                "content": {"uri": "root.b3dm"},
                "children": [
                    {
                        "boundingVolume": {"region": region_deg(0.2, 0.2, 0.8, 0.8)},
                        "content": {"uri": "tiles/in.b3dm"},
                        "children": []
                    },
                    {
                        "boundingVolume": {"region": region_deg(5.0, 5.0, 6.0, 6.0)},
                        "content": {"uri": "tiles/out.b3dm"},
                        "children": []
                    },
                    {
                        "boundingVolume": {"region": region_deg(0.0, 0.0, 1.0, 0.5)},
                        "contents": [{"uri": "tiles/multi1.glb"}, {"uri": "sub/nested-tileset.json"}]
                    }
                ]
            }
        });
        let mut keep = Vec::new();
        let out = filter_tileset(tileset, "tileset.json", &unit_polygon(), &mut keep);
        let children = out["root"]["children"].as_array().unwrap();
        assert_eq!(children.len(), 2, "non-intersecting child must be pruned");
        assert!(keep.contains(&"root.b3dm".to_string()));
        assert!(keep.contains(&"tiles/in.b3dm".to_string()));
        assert!(!keep.contains(&"tiles/out.b3dm".to_string()));
        assert!(keep.contains(&"tiles/multi1.glb".to_string()));
        assert!(keep.contains(&"sub/nested-tileset.json".to_string()));
    }

    #[test]
    fn filter_tileset_supports_pre10_content_url() {
        // Pre-1.0 tilesets (asset.version "0.0", e.g. OWT/Vricon exports) reference tile
        // payloads via `content.url`, not `content.uri`. Regression: these produced
        // clipped archives with zero tile content.
        let tileset = json!({
            "asset": {"version": "0.0"},
            "root": {
                "boundingVolume": {"region": region_deg(-1.0, -1.0, 2.0, 2.0)},
                "content": {"batchSize": 1, "url": "0/0/0.b3dm"},
                "children": [{
                    "boundingVolume": {"region": region_deg(0.2, 0.2, 0.8, 0.8)},
                    "content": {"batchSize": 1, "url": "1/1/0.b3dm"},
                    "children": []
                }]
            }
        });
        let mut keep = Vec::new();
        filter_tileset(tileset, "tileset.json", &unit_polygon(), &mut keep);
        assert!(keep.contains(&"0/0/0.b3dm".to_string()));
        assert!(keep.contains(&"1/1/0.b3dm".to_string()));
    }

    #[test]
    fn filter_tileset_root_kept_even_without_intersection_test() {
        // The root is never dropped: clipping only prunes below it.
        let tileset = json!({
            "root": {
                "boundingVolume": {"region": region_deg(50.0, 50.0, 51.0, 51.0)},
                "content": {"uri": "root.b3dm"},
                "children": []
            }
        });
        let mut keep = Vec::new();
        let out = filter_tileset(tileset, "tileset.json", &unit_polygon(), &mut keep);
        assert!(out["root"]["content"]["uri"].is_string());
        assert!(keep.contains(&"root.b3dm".to_string()));
    }

    // --- filter_i3s_scenelayer ---

    fn node(id: &str, lon: f64, lat: f64, radius_m: f64, children: &[&str]) -> I3SNode {
        I3SNode {
            id: id.to_string(),
            doc_filename: format!("nodes/{id}/3dNodeIndexDocument.json"),
            containing_doc: format!(
                "nodepages/{}.json",
                id.parse::<u64>().map(|n| n / 64).unwrap_or(0)
            ),
            mbs: [lon, lat, 0.0, radius_m],
            children: children
                .iter()
                .map(|c| ChildRef { id: c.to_string() })
                .collect(),
        }
    }

    #[test]
    fn i3s_traversal_keeps_intersecting_and_descends_through_missed_parents() {
        let mut all_nodes = HashMap::new();
        // Root far outside the polygon, but its child sits inside: traversal must not
        // spatially cull the child just because the parent missed.
        all_nodes.insert("0".to_string(), node("0", 50.0, 50.0, 10.0, &["1", "2"]));
        all_nodes.insert("1".to_string(), node("1", 0.5, 0.5, 100.0, &[]));
        all_nodes.insert("2".to_string(), node("2", 30.0, 30.0, 10.0, &[]));

        let scenelayer = json!({"store": {"rootNode": "./nodes/root"}});
        let mut keep_uris = HashSet::new();
        let mut kept_ids = HashSet::new();
        filter_i3s_scenelayer(
            &scenelayer,
            &all_nodes,
            &unit_polygon(),
            &mut keep_uris,
            &mut kept_ids,
        );

        assert!(
            kept_ids.contains("1"),
            "in-polygon child of an out-of-polygon parent must be kept"
        );
        assert!(!kept_ids.contains("2"));
        assert!(keep_uris.contains("nodes/1/3dNodeIndexDocument.json"));
    }

    #[test]
    fn i3s_prefers_node_page_root_zero() {
        let mut all_nodes = HashMap::new();
        all_nodes.insert("0".to_string(), node("0", 0.5, 0.5, 50.0, &[]));
        // A legacy "root" node also exists; "0" must win for 1.7+ node-page layouts.
        all_nodes.insert("root".to_string(), node("0", 0.5, 0.5, 50.0, &[]));
        let scenelayer = json!({"store": {"rootNode": "./nodes/root"}});
        let mut keep_uris = HashSet::new();
        let mut kept_ids = HashSet::new();
        filter_i3s_scenelayer(
            &scenelayer,
            &all_nodes,
            &unit_polygon(),
            &mut keep_uris,
            &mut kept_ids,
        );
        assert!(kept_ids.contains("0"));
    }

    // --- parse_node_bounds / mbs_to_rect / child_id_of ---

    #[test]
    fn parse_node_bounds_reads_mbs_and_both_obb_forms() {
        // I3S 1.6: mbs = [lon, lat, z, radius_m]
        let mbs = parse_node_bounds(&json!({"mbs": [10.0, 20.0, 5.0, 100.0]})).unwrap();
        assert_eq!(mbs, [10.0, 20.0, 5.0, 100.0]);

        // I3S 1.7+ obb as an object: radius is the half-diagonal of the box.
        let obb_obj = parse_node_bounds(&json!({
            "obb": {"center": [1.0, 2.0, 3.0], "halfSize": [3.0, 4.0, 12.0]}
        }))
        .unwrap();
        assert_eq!(&obb_obj[0..3], &[1.0, 2.0, 3.0]);
        assert!(
            (obb_obj[3] - 13.0).abs() < 1e-9,
            "half-diagonal of 3/4/12 is 13"
        );

        // obb as a flat array carries the same values positionally.
        let obb_arr = parse_node_bounds(&json!({"obb": [1.0, 2.0, 3.0, 3.0, 4.0, 12.0]})).unwrap();
        assert_eq!(obb_obj, obb_arr);
    }

    #[test]
    fn parse_node_bounds_rejects_missing_or_short_bounds() {
        assert!(parse_node_bounds(&json!({})).is_none());
        assert!(parse_node_bounds(&json!({"mbs": [1.0, 2.0]})).is_none());
        assert!(parse_node_bounds(&json!({"obb": [1.0, 2.0, 3.0]})).is_none());
        assert!(parse_node_bounds(&json!({"obb": {"center": [1.0, 2.0, 3.0]}})).is_none());
    }

    #[test]
    fn mbs_to_rect_brackets_the_centre() {
        // A 0-radius sphere degenerates to a point at its centre.
        let point = mbs_to_rect(&[10.0, 20.0, 0.0, 0.0]);
        assert_eq!((point.min().x, point.min().y), (10.0, 20.0));
        assert_eq!((point.max().x, point.max().y), (10.0, 20.0));

        // A real radius expands symmetrically, and further in longitude than latitude away
        // from the equator (degrees of longitude are shorter there).
        let rect = mbs_to_rect(&[0.0, 60.0, 0.0, 111_320.0]);
        assert!((rect.max().y - 61.0).abs() < 1e-6, "1 degree of latitude");
        assert!(
            rect.max().x > 1.9 && rect.max().x < 2.1,
            "~2 degrees of longitude at 60N"
        );
    }

    #[test]
    fn child_id_of_reads_every_reference_shape() {
        assert_eq!(child_id_of(&json!(42)).unwrap(), "42");
        assert_eq!(child_id_of(&json!("root")).unwrap(), "root");
        assert_eq!(child_id_of(&json!({"id": "1-2-3"})).unwrap(), "1-2-3");
        assert_eq!(child_id_of(&json!({"index": 7})).unwrap(), "7");
        assert!(child_id_of(&json!({"nothing": 1})).is_none());
        assert!(child_id_of(&json!(null)).is_none());
    }

    // --- expand_i3s_keep_set ---

    #[test]
    fn expand_keeps_resources_of_kept_nodes_only() {
        let kept: HashSet<String> = ["1".to_string()].into_iter().collect();
        let names = [
            "nodes/1/3dNodeIndexDocument.json",
            "nodes/1/geometries/0.bin",
            "nodes/1/textures/0_0.jpg",
            "nodes/1/shared/sharedResource.json",
            "nodes/2/geometries/0.bin",
            "nodepages/0.json",
            "statistics/summary.json",
            "metadata.json",
            "3dSceneLayer.json",
        ];
        let mut keep_uris = HashSet::new();
        let added = expand_i3s_keep_set(names.iter().copied(), &kept, &mut keep_uris);

        assert!(keep_uris.contains("nodes/1/geometries/0.bin"));
        assert!(keep_uris.contains("nodes/1/textures/0_0.jpg"));
        assert!(keep_uris.contains("nodes/1/shared/sharedResource.json"));
        assert!(
            !keep_uris.contains("nodes/2/geometries/0.bin"),
            "resources of dropped nodes must not be kept"
        );
        assert!(
            keep_uris.contains("nodepages/0.json"),
            "node pages are always kept"
        );
        assert!(keep_uris.contains("statistics/summary.json"));
        assert!(keep_uris.contains("metadata.json"));
        assert!(
            !keep_uris.contains("3dSceneLayer.json"),
            "scene layer is kept by the caller, not the expansion"
        );
        assert_eq!(added, keep_uris.len());
    }

    #[test]
    fn expand_handles_nested_layer_prefixes() {
        let kept: HashSet<String> = ["7".to_string()].into_iter().collect();
        let names = [
            "layers/0/nodes/7/geometries/0.bin",
            "layers/0/nodes/8/geometries/0.bin",
            "layers/0/nodepages/0.json",
        ];
        let mut keep_uris = HashSet::new();
        expand_i3s_keep_set(names.iter().copied(), &kept, &mut keep_uris);
        assert!(keep_uris.contains("layers/0/nodes/7/geometries/0.bin"));
        assert!(!keep_uris.contains("layers/0/nodes/8/geometries/0.bin"));
        assert!(keep_uris.contains("layers/0/nodepages/0.json"));
    }
}
