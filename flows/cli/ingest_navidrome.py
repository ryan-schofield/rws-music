#!/usr/bin/env python3
"""
CLI wrapper for Navidrome API ingestion via ListenBrainz.

Ingests recently played tracks from Navidrome (via ListenBrainz).
Usage: python flows/cli/ingest_navidrome.py
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.ingest.navidrome_api_ingestion import NavidromeDataIngestion


class NavidromeIngestionCLI(CLICommand):
    """CLI wrapper for Navidrome ingestion."""

    def __init__(self):
        super().__init__(
            name="navidrome_ingestion",
            timeout=300,  # 5 minutes
            retries=3,
        )
        self.ingestion = NavidromeDataIngestion()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute Navidrome ingestion via ListenBrainz.

        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info("Starting Navidrome ingestion via ListenBrainz")

            result = self.ingestion.run_ingestion()

            if result.get("status") == "success":
                return self.success_result(
                    message=f"Ingested {result.get('records_ingested', 0)} tracks",
                    data=result,
                )
            elif result.get("status") == "no_data":
                return self.success_result(
                    message="No new data to ingest",
                    data=result,
                )
            else:
                return self.error_result(
                    message="Navidrome ingestion failed",
                    errors=[result.get("message", "Unknown error")],
                )

        except Exception as e:
            self.logger.error(f"Navidrome ingestion error: {str(e)}")
            return self.error_result(
                message="Navidrome ingestion failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(
        description="Ingest recently played tracks from Navidrome via ListenBrainz"
    )

    args = parser.parse_args()

    cli = NavidromeIngestionCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
