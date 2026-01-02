#!/usr/bin/env python3
"""
CLI wrapper for parsing MusicBrainz artist data.

Processes raw JSON data from MusicBrainz API into structured format.
Usage: python flows/cli/parse_mbz_data.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.musicbrainz_processor import MusicBrainzProcessor


class ParseMBZDataCLI(CLICommand):
    """CLI wrapper for parsing MusicBrainz data."""

    def __init__(self):
        super().__init__(
            name="parse_mbz_data",
            timeout=600,  # 10 minutes
            retries=2,
        )
        self.processor = MusicBrainzProcessor()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute MusicBrainz data parsing.
        
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info("Starting MusicBrainz data parsing")
            
            result = self.processor.parse_json_files()
            
            if result.get("success"):
                return self.success_result(
                    message=f"Parsed {result.get('files_processed', 0)} MusicBrainz JSON files",
                    data=result,
                )
            else:
                return self.error_result(
                    message="MusicBrainz data parsing failed",
                    errors=[result.get("error", "Unknown error")],
                )
        
        except Exception as e:
            self.logger.error(f"MusicBrainz data parsing error: {str(e)}")
            return self.error_result(
                message="MusicBrainz data parsing failed",
                errors=[str(e)],
            )


def main():
    cli = ParseMBZDataCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
