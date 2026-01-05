#!/usr/bin/env python3
"""
CLI wrapper for Spotify artist enrichment.

Enriches missing artist data from Spotify API.
Usage: python flows/cli/enrich_spotify_artists.py --limit 50
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


class EnrichSpotifyArtistsCLI(CLICommand):
    """CLI wrapper for Spotify artist enrichment."""

    def __init__(self):
        super().__init__(
            name="enrich_spotify_artists",
            timeout=600,  # 10 minutes
            retries=3,
        )
        self.processor = SpotifyProcessor()

    def execute(self, limit: int = None, **kwargs) -> Dict[str, Any]:
        """
        Execute Spotify artist enrichment.

        Args:
            limit: Maximum number of artists to enrich

        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info(f"Starting Spotify artist enrichment with limit={limit}")

            result = self.processor.enrich_artists(limit=limit)

            if result.get("status") == "success":
                return self.success_result(
                    message=f"Enriched {result.get('artists_processed', 0)} artists",
                    data=result,
                )
            elif result.get("status") == "no_updates":
                return self.no_updates_result(
                    result.get("message", "No artists to process")
                )
            else:
                return self.error_result(
                    message="Spotify artist enrichment failed",
                    errors=[result.get("message", "Unknown error")],
                )

        except Exception as e:
            self.logger.error(f"Spotify artist enrichment error: {str(e)}")
            return self.error_result(
                message="Spotify artist enrichment failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(description="Enrich Spotify artist data")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of artists to enrich",
    )

    args = parser.parse_args()

    cli = EnrichSpotifyArtistsCLI()
    exit_code = cli.run(limit=args.limit)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
