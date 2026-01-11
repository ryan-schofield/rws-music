#!/usr/bin/env python3
"""
CLI wrapper for consolidating raw track data into CSV.

Consolidates all JSON files from recently_played/detail directory to a single CSV.
Usage: python flows/cli/consolidate_tracks.py
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand


class ConsolidateTracksCLI(CLICommand):
    """CLI wrapper for consolidating track data."""

    def __init__(self):
        super().__init__(
            name="consolidate_tracks",
            timeout=300,  # 5 minutes
            retries=3,
        )

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute consolidation.

        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info(f"Starting track data consolidation")

            # Import here to avoid circular imports
            from flows.ingest.spotify_api_ingestion import SpotifyDataIngestion

            ingestion = SpotifyDataIngestion()
            csv_file = ingestion.consolidate_to_csv()

            if csv_file:
                return self.success_result(
                    message=f"Consolidated track data to CSV",
                    data={"csv_file": csv_file},
                )
            else:
                return self.no_updates_result(
                    message="No data to consolidate",
                )

        except Exception as e:
            self.logger.error(f"Consolidation error: {str(e)}")
            return self.error_result(
                message="Consolidation failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(description="Consolidate raw track data into CSV")

    args = parser.parse_args()

    cli = ConsolidateTracksCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
