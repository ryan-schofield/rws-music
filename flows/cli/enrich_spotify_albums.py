#!/usr/bin/env python3
"""
CLI wrapper for Spotify album enrichment.

Enriches missing album data from Spotify API.
Usage: python flows/cli/enrich_spotify_albums.py --limit 50
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.spotify_processor import SpotifyProcessor


class EnrichSpotifyAlbumsCLI(CLICommand):
    """CLI wrapper for Spotify album enrichment."""

    def __init__(self):
        super().__init__(
            name="enrich_spotify_albums",
            timeout=600,  # 10 minutes
            retries=3,
        )
        self.processor = SpotifyProcessor()

    def execute(self, limit: int = None, **kwargs) -> Dict[str, Any]:
        """
        Execute Spotify album enrichment.
        
        Args:
            limit: Maximum number of albums to enrich
            
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info(f"Starting Spotify album enrichment with limit={limit}")
            
            result = self.processor.enrich_albums(limit=limit)
            
            if result.get("status") == "success":
                return self.success_result(
                    message=f"Enriched {result.get('albums_processed', 0)} albums",
                    data=result,
                )
            else:
                return self.error_result(
                    message="Spotify album enrichment failed",
                    errors=[result.get("message", "Unknown error")],
                )
        
        except Exception as e:
            self.logger.error(f"Spotify album enrichment error: {str(e)}")
            return self.error_result(
                message="Spotify album enrichment failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(description="Enrich Spotify album data")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of albums to enrich",
    )
    
    args = parser.parse_args()
    
    cli = EnrichSpotifyAlbumsCLI()
    exit_code = cli.run(limit=args.limit)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
