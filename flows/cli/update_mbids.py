#!/usr/bin/env python3
"""
CLI wrapper for updating MusicBrainz IDs in Spotify data.

Updates Spotify artist records with MusicBrainz IDs.
Usage: python flows/cli/update_mbids.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.spotify_processor import SpotifyProcessor


class UpdateMBIDsCLI(CLICommand):
    """CLI wrapper for updating MusicBrainz IDs."""

    def __init__(self):
        super().__init__(
            name="update_mbids",
            timeout=600,  # 10 minutes
            retries=3,
        )
        self.processor = SpotifyProcessor()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute MusicBrainz ID update.

        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info("Starting MusicBrainz ID update")

            result = self.processor.update_artist_mbids()

            if result.get("status") == "success":
                return self.success_result(
                    message="Updated artist MBIDs successfully",
                    data=result,
                )
            elif result.get("status") == "no_updates":
                return self.no_updates_result(
                    result.get("message", "No MBIDs to update")
                )
            else:
                return self.error_result(
                    message="MusicBrainz ID update failed",
                    errors=[result.get("message", "Unknown error")],
                )

        except Exception as e:
            self.logger.error(f"MusicBrainz ID update error: {str(e)}")
            return self.error_result(
                message="MusicBrainz ID update failed",
                errors=[str(e)],
            )


def main():
    cli = UpdateMBIDsCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
