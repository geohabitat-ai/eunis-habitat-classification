import asyncio
import logging
import json
import httpx
import geopandas as gpd
from pathlib import Path
from typing import List, Dict, Any, AsyncGenerator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
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

    async def get_metadata(self, service_path: str) -> Dict:
        """Fetches service-level metadata to determine server limits."""
        # We query the MapServer root, not the layer (layer/0), as limits are usually defined there.
        url = f"{self.base_url}/{service_path}/MapServer?f=json"
        return await self._req("GET", url)

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

    async def download_stream(self, service_path: str, layer_id: int = 0) -> AsyncGenerator[List[Dict], None]:
        """
        Yields data chunks, dynamically sized to respect the server's maxRecordCount.
        """
        # 1. Metadata Handshake (Systematic Fix)
        # We fetch metadata to find the specific limit for THIS service (e.g., 1000 vs 2000)
        meta = await self.get_metadata(service_path)
        
        # Default to 1000 if undefined (safest common default for ArcGIS), 
        # but trust the server if it reports a higher/lower number.
        server_limit = meta.get("maxRecordCount", 1000)
        
        logger.info(f"[{service_path}] Server limit: {server_limit} records per request.")

        # 2. Fetch all Object IDs
        url = f"{self.base_url}/{service_path}/MapServer/{layer_id}/query"
        id_params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
        
        id_data = await self._req("GET", url, params=id_params)
        object_ids = id_data.get("objectIds", [])

        if not object_ids:
            logger.warning(f"[{service_path}] No features found.")
            return

        total_features = len(object_ids)
        logger.info(f"[{service_path}] Found {total_features} features. Starting stream...")

        # 3. Chunking Strategy
        # We use the discovered server_limit as the chunk size.
        # This guarantees we never send more IDs than the server can process.
        for i in range(0, total_features, server_limit):
            chunk_ids = object_ids[i : i + server_limit]
            yield await self._fetch_chunk(url, chunk_ids)

    async def _fetch_chunk(self, url: str, object_ids: List[int]) -> List[Dict]:
        """Fetches a specific chunk by ID."""
        data_payload = {
            "objectIds": ",".join(map(str, object_ids)),
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson"
        }
        data = await self._req("POST", url, data=data_payload)
        return data.get("features", [])

    async def download_all_datasets(self, output_dir: str = "data/raw/eunis") -> None:
        """Downloads all MapServer services to disk."""
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        services = await self.list_services()
        map_services = [s for s in services if s.get("type") == "MapServer"]
        
        logger.info(f"Found {len(map_services)} MapServer datasets.")

        for service in map_services:
            service_name = service["name"]
            # Prevent overwrites by using full path with underscores
            safe_name = service_name.replace("/", "_") 
            file_path = out_path / f"{safe_name}.geojson"

            logger.info(f"Streaming {service_name} to {file_path}...")

            # Stream write to file (avoids OOM on large datasets)
            try:
                with open(file_path, "w") as f:
                    # Write GeoJSON header
                    f.write('{"type": "FeatureCollection", "features": [')
                    
                    first_chunk = True
                    feature_count = 0
                    
                    async for batch in self.download_stream(service_name):
                        if not batch:
                            continue
                            
                        # Convert batch (list of dicts) to JSON string, but strip outer brackets
                        # or iterate to handle commas correctly. Iteration is safer.
                        for feature in batch:
                            if not first_chunk:
                                f.write(',')
                            else:
                                first_chunk = False
                            
                            json.dump(feature, f)
                            feature_count += 1
                            
                    # Write GeoJSON footer
                    f.write(']}')
                    
                logger.info(f"Finished {safe_name}: {feature_count} features.")
                
            except Exception as e:
                logger.error(f"Failed to download {service_name}: {e}")
                # Clean up partial file
                if file_path.exists():
                    file_path.unlink()