#!/usr/bin/env python3
"""
CLI wrapper for processing MusicBrainz area hierarchy data.

Builds geographic hierarchy from MusicBrainz area data.
Usage: python flows/cli/process_mbz_hierarchy.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.musicbrainz_processor import MusicBrainzProcessor


class ProcessMBZHierarchyCLI(CLICommand):
    """CLI wrapper for processing MusicBrainz area hierarchy."""

    def __init__(self):
        super().__init__(
            name="process_mbz_hierarchy",
            timeout=600,  # 10 minutes
            retries=2,
        )
        self.processor = MusicBrainzProcessor()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute MusicBrainz area hierarchy processing.
        
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info("Starting MusicBrainz area hierarchy processing")
            
            result = self.processor.process_area_hierarchy()
            
            if result.get("status") == "success":
                return self.success_result(
                    message=f"Processed {result.get('areas_processed', 0)} geographic areas",
                    data=result,
                )
            else:
                return self.error_result(
                    message="MusicBrainz area hierarchy processing failed",
                    errors=[result.get("message", "Unknown error")],
                )
        
        except Exception as e:
            self.logger.error(f"MusicBrainz area hierarchy processing error: {str(e)}")
            return self.error_result(
                message="MusicBrainz area hierarchy processing failed",
                errors=[str(e)],
            )


def main():
    cli = ProcessMBZHierarchyCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
