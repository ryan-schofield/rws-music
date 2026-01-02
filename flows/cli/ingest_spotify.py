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
from flows.ingest.spotify_api_ingestion import SpotifyAPIIngestion


class SpotifyIngestionCLI(CLICommand):
    """CLI wrapper for Spotify ingestion."""

    def __init__(self):
        super().__init__(
            name="spotify_ingestion",
            timeout=300,  # 5 minutes
            retries=3,
        )
        self.ingestion = SpotifyAPIIngestion()

    def execute(self, limit: int = None, **kwargs) -> Dict[str, Any]:
        """
        Execute Spotify ingestion.
        
        Args:
            limit: Maximum number of tracks to ingest per batch
            
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info(f"Starting Spotify ingestion with limit={limit}")
            
            result = self.ingestion.ingest_recently_played(limit=limit)
            
            if result.get("success"):
                return self.success_result(
                    message=f"Ingested {result.get('tracks_count', 0)} tracks",
                    data=result,
                )
            else:
                return self.error_result(
                    message="Spotify ingestion failed",
                    errors=[result.get("error", "Unknown error")],
                )
        
        except Exception as e:
            self.logger.error(f"Spotify ingestion error: {str(e)}")
            return self.error_result(
                message="Spotify ingestion failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(description="Ingest recently played tracks from Spotify")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of tracks to ingest per batch",
    )
    
    args = parser.parse_args()
    
    cli = SpotifyIngestionCLI()
    exit_code = cli.run(limit=args.limit)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
