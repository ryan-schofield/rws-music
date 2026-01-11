#!/usr/bin/env python3
"""
Standalone CLI wrapper for Navidrome API ingestion via ListenBrainz.

Ingests recently played tracks from Navidrome (via ListenBrainz) without
requiring the full CLI framework dependencies.
Usage: python flows/cli/ingest_navidrome.py
"""

import sys
import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Load .env file from project root - override=True to replace existing env vars
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)
    print(f"Loaded .env from {env_path} (override=True)")
else:
    print(f".env file not found at {env_path}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    try:
        from flows.ingest.navidrome_api_ingestion import NavidromeDataIngestion

        logger.info("Starting Navidrome ingestion via ListenBrainz")

        ingestor = NavidromeDataIngestion()
        result = ingestor.run_ingestion()

        # Print result for logging/monitoring
        print(json.dumps(result, indent=2, default=str))

        # Exit with appropriate code
        if result.get("status") == "success" or result.get("status") == "no_data":
            return 0
        else:
            return 1

    except Exception as e:
        logger.error(f"Navidrome ingestion failed: {e}")
        error_result = {"status": "error", "message": str(e)}
        print(json.dumps(error_result, indent=2))
        return 1


if __name__ == "__main__":
    sys.exit(main())
