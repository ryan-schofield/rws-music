#!/usr/bin/env python3
"""
CLI wrapper for discovering MusicBrainz artists.

Discovers artists that need MusicBrainz enrichment.
Usage: python flows/cli/discover_mbz_artists.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.musicbrainz_processor import MusicBrainzProcessor


class DiscoverMBZArtistsCLI(CLICommand):
    """CLI wrapper for discovering MusicBrainz artists."""

    def __init__(self):
        super().__init__(
            name="discover_mbz_artists",
            timeout=600,  # 10 minutes
            retries=2,
        )
        self.processor = MusicBrainzProcessor()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute artist discovery for MusicBrainz enrichment.
        
        Returns:
            Result dictionary with list of artists needing enrichment
        """
        try:
            self.logger.info("Starting MusicBrainz artist discovery")
            
            result = self.processor.discover_missing_artists()
            
            if result.get("success"):
                return self.success_result(
                    message=f"Found {result.get('artists_count', 0)} artists needing enrichment",
                    data=result,
                )
            else:
                return self.no_updates_result(
                    message="No artists need MusicBrainz enrichment",
                )
        
        except Exception as e:
            self.logger.error(f"MusicBrainz artist discovery error: {str(e)}")
            return self.error_result(
                message="MusicBrainz artist discovery failed",
                errors=[str(e)],
            )


def main():
    cli = DiscoverMBZArtistsCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
