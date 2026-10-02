"""
Admission checks and placement policies, shared by the live manager
(manager.py) and the discrete-event simulator (simulate.py) so both
make identical decisions.
"""
from . import util

POLICIES = ["edge_first", "least_loaded", "best_fit", "latency_slack"]


def evaluate_sites(sites, topology, ingress, demand, max_latency):
    """Check every ACTIVE site against the three admission conditions
    (latency, node capacity, link bandwidth). Returns (candidates, fail_reasons)."""
    candidates, reasons = [], []
    for sid, site in sites.items():
        if site["status"] != "ACTIVE":
            continue
        path, lat = topology.shortest_path(ingress, sid)
        if path is None:
            reasons.append("unreachable")
            continue
        if lat > max_latency:
            reasons.append("latency")
            continue
        if not util.fits(site["allocated"], demand, site["capacity"]):
            reasons.append("node_capacity")
            continue
        ok, _ = topology.path_has_bandwidth(path, demand["bw"])
        if not ok:
            reasons.append("link_bandwidth")
            continue
        after = {r: site["allocated"][r] + demand.get(r, 0) for r in site["capacity"]}
        candidates.append({
            "site_id": sid,
            "tier": site["tier"],
            "path": path,
            "path_latency_ms": lat,
            "dominant_share_after": util.dominant_share(after, site["capacity"]),
            "dominant_free_share_after": util.dominant_free_share(after, site["capacity"]),
        })
    return candidates, reasons


def pick_reason(reasons):
    """Label a block by the *binding* constraint: the furthest admission
    stage any site reached (latency -> node_capacity -> link_bandwidth).
    A majority vote would call a full ingress edge "latency" merely because
    the other sites are too far away; furthest-stage attribution says what
    would actually have to change for the request to fit."""
    if not reasons:
        return "no_active_sites"
    stage = {"unreachable": 0, "latency": 1, "node_capacity": 2, "link_bandwidth": 3}
    return max(reasons, key=lambda r: stage.get(r, -1))


def choose(policy, candidates):
    if policy == "least_loaded":
        key = lambda c: c["dominant_share_after"]
    elif policy == "best_fit":
        key = lambda c: c["dominant_free_share_after"]
    elif policy == "latency_slack":
        # Push latency-tolerant work to the core first, so scarce edge
        # capacity stays free for services that can ONLY run at the edge
        # (e.g. the 2 ms vRAN-DU). Strict services have no core candidate,
        # so they fall through to the ingress edge automatically.
        key = lambda c: (0 if c["tier"] == "core" else 1,
                         c["path_latency_ms"], c["dominant_share_after"])
    else:  # edge_first
        key = lambda c: (c["path_latency_ms"], c["dominant_share_after"])
    return min(candidates, key=key)
