#!/usr/bin/env python3
"""
CLI wrapper for geography base enrichment (continents + params).

Adds continent information and geocoding parameters without making API calls.
Fast operation suitable for simple wrapper workflow.

Usage: python flows/cli/enrich_geography_base.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.geo_processor import GeographicProcessor


class EnrichGeographyBaseCLI(CLICommand):
    """CLI wrapper for base geography enrichment."""

    def __init__(self):
        super().__init__(
            name="enrich_geography_base",
            timeout=600,  # 10 minutes
            retries=2,
        )
        self.processor = GeographicProcessor()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute base geography enrichment.

        Steps:
        1. Add continent info (pycountry)
        2. Add geocoding params

        Returns:
            Result dictionary with enrichment metrics
        """
        try:
            self.logger.info("Starting base geography enrichment")

            result = self.processor.enrich_base()

            if result.get("status") == "success":
                return self.success_result(
                    message="Base geography enrichment completed",
                    data=result,
                )
            else:
                return self.no_updates_result(
                    message="No geography base enrichment needed",
                )

        except Exception as e:
            self.logger.error(f"Geography base enrichment error: {str(e)}")
            return self.error_result(
                message="Geography base enrichment failed",
                errors=[str(e)],
            )


def main():
    cli = EnrichGeographyBaseCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
