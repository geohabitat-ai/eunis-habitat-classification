import asyncio
import logging
import sys
from pathlib import Path

# Ensure src is in path
sys.path.append(str(Path(__file__).parent.parent))
from src.eunis_client import EunisClient

# Configure logging for the pipeline execution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def download_stage():
    """Stage 1: Ingest Raw Data"""
    logger.info(">>> Starting Pipeline Stage 1: Download")
    
    # Using the client context manager ensures the session closes properly
    async with EunisClient() as client:
        # We can pass specific output directories or configs here
        await client.download_all_datasets(output_dir="data/raw/eunis")
    
    logger.info("<<< Stage 1 Complete")

async def processing_stage():
    """Stage 2: Process Data (Placeholder for your future logic)"""
    logger.info(">>> Starting Pipeline Stage 2: Processing")
    
    raw_path = Path("data/raw/eunis")
    if not raw_path.exists() or not any(raw_path.iterdir()):
        logger.error("No raw data found. Aborting.")
        return

    # TODO: Add logic to combine GeoDataFrames or filter N/A values here
    # Example:
    # for file in raw_path.glob("*.geojson"):
    #     process_file(file)
        
    logger.info("<<< Stage 2 Complete (Skipped/TODO)")

async def main():
    try:
        # Run stages sequentially
        await download_stage()
        await processing_stage()
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())