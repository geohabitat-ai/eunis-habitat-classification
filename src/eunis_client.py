import asyncio
import logging
from typing import List, Dict, Any, Optional

import httpx
import geopandas as gpd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging (application should ideally configure this, but we provide defaults)
logger = logging.getLogger(__name__)

class EunisClient:
    def __init__(self, base_url: str = "https://bio.discomap.eea.europa.eu/arcgis/rest/services", concurrency: int = 5):
        self.base_url = base_url.rstrip("/")
        self.semaphore = asyncio.Semaphore(concurrency)
        self.client = httpx.AsyncClient(timeout=60.0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.TimeoutException))
    )
    async def _req(self, method: str, url: str, **kwargs) -> Dict:
        """Centralized request handler. Retries on network errors; raises on API errors."""
        async with self.semaphore:
            response = await self.client.request(method, url, **kwargs)
            response.raise_for_status()

            data = response.json()
            if "error" in data:
                # Fail loudly if the API returns a logic error (e.g. 200 OK but "query limit exceeded")
                raise ValueError(f"ArcGIS API Error: {data['error']}")
            return data

    async def list_services(self, folder: str = "EUNIS") -> List[Dict[str, Any]]:
        """Lists services in the specified folder."""
        url = f"{self.base_url}/{folder}?f=json"
        data = await self._req("GET", url)
        return data.get("services", [])

    async def get_preview(self, service_path: str, layer_id: int = 0, limit: int = 1000) -> gpd.GeoDataFrame:
        """Fetches the first N rows for quick exploration."""
        url = f"{self.base_url}/{service_path}/MapServer/{layer_id}/query"

        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "resultRecordCount": limit,
            "f": "geojson"
        }

        logger.info(f"Previewing {limit} rows from {service_path}/{layer_id}...")
        data = await self._req("GET", url, params=params)

        if "features" in data and data["features"]:
            return gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")

        return gpd.GeoDataFrame()

    async def download_layer(self, service_path: str, layer_id: int = 0) -> gpd.GeoDataFrame:
        """
        Downloads full dataset.

        Strategy:
        1. Fetch all Object IDs (lightweight).
        2. Chunk IDs and fetch features in parallel (heavyweight).
        3. Strict Error Handling: If ANY chunk fails, the whole process raises an exception.
        """
        # 1. Get all IDs
        url = f"{self.base_url}/{service_path}/MapServer/{layer_id}/query"
        logger.info(f"Fetching IDs for {service_path}/{layer_id}...")

        id_params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
        id_data = await self._req("GET", url, params=id_params)
        object_ids = id_data.get("objectIds", [])

        if not object_ids:
            logger.warning(f"No features found for {service_path}.")
            return gpd.GeoDataFrame()

        total_features = len(object_ids)
        logger.info(f"Found {total_features} features. Starting download...")

        # 2. Create Tasks
        chunk_size = 1000
        tasks = []
        for i in range(0, total_features, chunk_size):
            chunk_ids = object_ids[i : i + chunk_size]
            tasks.append(self._fetch_chunk(url, chunk_ids))

        # 3. Execute & Gather
        # asyncio.gather ensures that if one task fails, the exception propagates up.
        results = await asyncio.gather(*tasks)

        # 4. Flatten
        all_features = [feat for batch in results for feat in batch]

        # Validation: Ensure we actually got what we asked for
        if len(all_features) != total_features:
            logger.error(f"Mismatch: Expected {total_features}, got {len(all_features)}")
            # Decision: In strict mode, we might raise here. For now, just log.

        logger.info(f"Download complete. Constructing GeoDataFrame with {len(all_features)} rows.")
        return gpd.GeoDataFrame.from_features(all_features, crs="EPSG:4326")

    async def _fetch_chunk(self, url: str, object_ids: List[int]) -> List[Dict]:
        """Fetches raw GeoJSON features for a chunk of IDs. No error suppression."""
        data_payload = {
            "objectIds": ",".join(map(str, object_ids)),
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson"
        }
        data = await self._req("POST", url, data=data_payload)
        return data.get("features", [])