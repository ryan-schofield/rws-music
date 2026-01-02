#!/usr/bin/env python3
"""
CLI wrapper for loading raw track data.

Loads raw JSON track data from Spotify API into structured format.
Usage: python flows/cli/load_raw_tracks.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.load.append_tracks import main as append_tracks_main


class LoadRawTracksCLI(CLICommand):
    """CLI wrapper for loading raw tracks."""

    def __init__(self):
        super().__init__(
            name="load_raw_tracks",
            timeout=300,  # 5 minutes
            retries=1,
        )

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute raw tracks loading.
        
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info("Starting raw tracks data loading")
            
            # Call existing append_tracks logic
            append_tracks_main()
            
            return self.success_result(
                message="Raw tracks data loaded successfully",
            )
        
        except Exception as e:
            self.logger.error(f"Raw tracks loading error: {str(e)}")
            return self.error_result(
                message="Raw tracks loading failed",
                errors=[str(e)],
            )


def main():
    cli = LoadRawTracksCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
