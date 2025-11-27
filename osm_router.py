"""OSMnx-based local routing helper for Charlottesville / UVA area.

This module builds (or loads) a drivable OpenStreetMap graph for the
OnDemand service area and exposes a minimal interface for routing between
latitude/longitude pairs. Graph data is cached under /data by default so
startup does not repeatedly hit the Overpass/OSM APIs.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import networkx as nx
import osmnx as ox

# ---------------------------
# Configuration
# ---------------------------
# Tweak these bounds to refine the service area without touching the rest of
# the routing code. Bounds roughly cover Charlottesville / UVA.
BBOX_NORTH = float(os.getenv("OSM_ROUTER_BBOX_NORTH", "38.12"))
BBOX_SOUTH = float(os.getenv("OSM_ROUTER_BBOX_SOUTH", "37.99"))
BBOX_EAST = float(os.getenv("OSM_ROUTER_BBOX_EAST", "-78.43"))
BBOX_WEST = float(os.getenv("OSM_ROUTER_BBOX_WEST", "-78.60"))

# Optional place name fallback (unused unless graph_from_place is preferred)
PLACE_NAME = os.getenv("OSM_ROUTER_PLACE", "Charlottesville, Virginia, USA")

# Primary data directory mirrors the rest of the app's storage pattern
DATA_DIR = Path(os.getenv("OSM_ROUTER_DATA_DIR", "/data"))
GRAPH_FILENAME = os.getenv("OSM_ROUTER_GRAPH_FILENAME", "osmnx_drive.graphml")
GRAPH_PATH = DATA_DIR / GRAPH_FILENAME


@dataclass
class RouteResult:
    coordinates: List[Tuple[float, float]]
    distance_meters: float
    travel_time_seconds: Optional[float]


class LocalOSMRouter:
    """Thin wrapper around an OSMnx drive graph for local routing."""

    def __init__(self, graph_path: Path = GRAPH_PATH) -> None:
        self.graph_path = graph_path
        self.graph: Optional[nx.MultiDiGraph] = None
        self.graph_path.parent.mkdir(parents=True, exist_ok=True)

    # Public API -----------------------------------------------------
    def ensure_graph(self) -> nx.MultiDiGraph:
        """Load the cached graph or build it once from OSM."""

        if self.graph is not None:
            return self.graph
        if self.graph_path.exists():
            self.graph = ox.load_graphml(self.graph_path)
            return self.graph

        self.graph = self._build_graph()
        ox.save_graphml(self.graph, self.graph_path)
        return self.graph

    def has_graph(self) -> bool:
        return self.graph is not None or self.graph_path.exists()

    def shortest_path(
        self,
        origin: Tuple[float, float],
        destination: Tuple[float, float],
    ) -> RouteResult:
        graph = self.ensure_graph()
        if graph is None:
            raise RuntimeError("routing graph unavailable")

        origin_lat, origin_lng = origin
        dest_lat, dest_lng = destination
        try:
            origin_node = ox.distance.nearest_nodes(graph, origin_lng, origin_lat)
            dest_node = ox.distance.nearest_nodes(graph, dest_lng, dest_lat)
        except Exception as exc:
            raise ValueError(f"failed to snap to road network: {exc}")

        try:
            path = nx.shortest_path(graph, origin_node, dest_node, weight="length")
        except nx.NetworkXNoPath as exc:
            raise ValueError(f"no route found: {exc}")

        coords, distance_m, travel_time_s = self._path_coordinates(graph, path)
        return RouteResult(
            coordinates=coords,
            distance_meters=distance_m,
            travel_time_seconds=travel_time_s,
        )

    # Internal helpers ----------------------------------------------
    def _build_graph(self) -> nx.MultiDiGraph:
        """Fetch a fresh drivable graph covering the configured bounding box."""

        # Use bounding box to avoid accidentally pulling far-away geometry.
        graph = ox.graph_from_bbox(
            BBOX_NORTH,
            BBOX_SOUTH,
            BBOX_EAST,
            BBOX_WEST,
            network_type="drive",
            simplify=True,
        )
        graph = ox.add_edge_speeds(graph)
        graph = ox.add_edge_travel_times(graph)
        return graph

    def _path_coordinates(
        self, graph: nx.MultiDiGraph, path: Sequence[int]
    ) -> Tuple[List[Tuple[float, float]], float, Optional[float]]:
        coords: List[Tuple[float, float]] = []
        distance_m = 0.0
        travel_time_s: Optional[float] = 0.0

        for u, v in zip(path[:-1], path[1:]):
            edge_data = graph.get_edge_data(u, v, default={})
            segment = self._choose_edge(edge_data)
            distance_m += float(segment.get("length", 0.0) or 0.0)
            travel_time_s = (travel_time_s or 0.0) + float(
                segment.get("travel_time", 0.0) or 0.0
            )
            segment_coords = self._edge_coordinates(graph, u, v, segment)
            if not coords:
                coords.extend(segment_coords)
            else:
                coords.extend(segment_coords[1:])

        return coords, distance_m, travel_time_s if travel_time_s else None

    def _choose_edge(self, edge_data: dict) -> dict:
        if not edge_data:
            return {}
        if len(edge_data) == 1:
            return next(iter(edge_data.values()))
        return min(edge_data.values(), key=lambda d: d.get("length", float("inf")))

    def _edge_coordinates(
        self, graph: nx.MultiDiGraph, u: int, v: int, data: dict
    ) -> List[Tuple[float, float]]:
        geometry = data.get("geometry") if isinstance(data, dict) else None
        if geometry is not None:
            points = [(lat, lon) for lon, lat in geometry.coords]
            return points

        u_lat = graph.nodes[u].get("y")
        u_lon = graph.nodes[u].get("x")
        v_lat = graph.nodes[v].get("y")
        v_lon = graph.nodes[v].get("x")
        return [(u_lat, u_lon), (v_lat, v_lon)]


__all__ = [
    "LocalOSMRouter",
    "RouteResult",
    "GRAPH_PATH",
    "DATA_DIR",
]
