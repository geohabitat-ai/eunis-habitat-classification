# run_pipeline.py
import asyncio
import logging
from eunis_client import EunisClient

# Configure standard logging for the script
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_Pipeline")

async def sync_eunis_data(service_name: str, output_path: str):
    """
    Orchestrates the download and save process.
    """
    logger.info(f"Job started for {service_name}")

    try:
        async with EunisClient(concurrency=10) as client:
            # 1. Download
            gdf = await client.download_layer(service_name)

            if gdf.empty:
                logger.warning("Dataset was empty. Skipping save.")
                return

            # 2. Transform (Example: Reproject to Web Mercator)
            if gdf.crs != "EPSG:3857":
                logger.info("Reprojecting to EPSG:3857...")
                gdf = gdf.to_crs("EPSG:3857")

            # 3. Load / Save
            logger.info(f"Saving to {output_path}...")
            gdf.to_file(output_path, driver="GPKG")
            logger.info("Job Success.")

    except Exception as e:
        logger.error(f"Job Failed: {e}", exc_info=True)
        # In an API context, you might re-raise this to trigger a 500 response
        raise

if __name__ == "__main__":
    # Simulate CLI execution
    asyncio.run(sync_eunis_data("EUNIS/Grassland_Distribution_point", "data/grasslands_processed.gpkg"))