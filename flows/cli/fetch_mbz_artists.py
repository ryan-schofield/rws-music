#!/usr/bin/env python3
"""
CLI wrapper for fetching MusicBrainz artist data.

Fetches artist data from MusicBrainz API for discovered artists.
Usage: python flows/cli/fetch_mbz_artists.py --limit 100 --workers 5
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.musicbrainz_processor import MusicBrainzProcessor


class FetchMBZArtistsCLI(CLICommand):
    """CLI wrapper for fetching MusicBrainz artist data."""

    def __init__(self):
        super().__init__(
            name="fetch_mbz_artists",
            timeout=900,  # 15 minutes
            retries=3,
        )
        self.processor = MusicBrainzProcessor()

    def execute(self, limit: int = None, max_workers: int = 5, **kwargs) -> Dict[str, Any]:
        """
        Execute MusicBrainz artist data fetching.
        
        Args:
            limit: Maximum number of artists to fetch
            max_workers: Number of parallel workers for API calls
            
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info(
                f"Starting MusicBrainz artist fetch with limit={limit}, workers={max_workers}"
            )
            
            result = self.processor.fetch_artist_data(limit=limit, max_workers=max_workers)
            
            if result.get("success"):
                return self.success_result(
                    message=f"Fetched {result.get('artists_fetched', 0)} artists from MusicBrainz",
                    data=result,
                )
            else:
                return self.error_result(
                    message="MusicBrainz artist fetch failed",
                    errors=[result.get("error", "Unknown error")],
                )
        
        except Exception as e:
            self.logger.error(f"MusicBrainz artist fetch error: {str(e)}")
            return self.error_result(
                message="MusicBrainz artist fetch failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(description="Fetch MusicBrainz artist data")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of artists to fetch",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of parallel workers",
    )
    
    args = parser.parse_args()
    
    cli = FetchMBZArtistsCLI()
    exit_code = cli.run(limit=args.limit, max_workers=args.workers)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
