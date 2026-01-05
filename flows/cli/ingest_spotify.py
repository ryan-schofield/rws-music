#!/usr/bin/env python3
"""
CLI wrapper for Spotify API ingestion.

Ingests recently played tracks from Spotify API.
Usage: python flows/cli/ingest_spotify.py --limit 50
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.ingest.spotify_api_ingestion import SpotifyDataIngestion


class SpotifyIngestionCLI(CLICommand):
    """CLI wrapper for Spotify ingestion."""

    def __init__(self):
        super().__init__(
            name="spotify_ingestion",
            timeout=300,  # 5 minutes
            retries=3,
        )
        self.ingestion = SpotifyDataIngestion()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute Spotify ingestion.

        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info(f"Starting Spotify ingestion")

            result = self.ingestion.run_ingestion()

            if result.get("status") == "success":
                return self.success_result(
                    message=f"Ingested {result.get('records_ingested', 0)} tracks",
                    data=result,
                )
            else:
                return self.error_result(
                    message="Spotify ingestion failed",
                    errors=[result.get("message", "Unknown error")],
                )

        except Exception as e:
            self.logger.error(f"Spotify ingestion error: {str(e)}")
            return self.error_result(
                message="Spotify ingestion failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(
        description="Ingest recently played tracks from Spotify"
    )

    args = parser.parse_args()

    cli = SpotifyIngestionCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
