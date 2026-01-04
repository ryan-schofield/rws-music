#!/usr/bin/env python3
"""
CLI wrapper for geographic enrichment processing.

Enriches geographic data with continent mapping and latitude/longitude.
Usage: python flows/cli/enrich_geography.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.geo_processor import GeographicProcessor


class EnrichGeographyCLI(CLICommand):
    """CLI wrapper for geographic enrichment."""

    def __init__(self):
        super().__init__(
            name="enrich_geography",
            timeout=1800,  # 30 minutes
            retries=0,  # No retries for data integrity critical task
        )
        self.processor = GeographicProcessor()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute geographic enrichment.
        
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info("Starting geographic enrichment")
            
            # Note: processor has internal parallelization
            result = self.processor.run_full_enrichment()
            
            if result.get("overall_status") == "success":
                return self.success_result(
                    message="Geographic enrichment completed successfully",
                    data=result,
                )
            else:
                return self.error_result(
                    message="Geographic enrichment failed",
                    errors=[result.get("message", "Unknown error")],
                )
        
        except Exception as e:
            self.logger.error(f"Geographic enrichment error: {str(e)}")
            return self.error_result(
                message="Geographic enrichment failed",
                errors=[str(e)],
            )


def main():
    cli = EnrichGeographyCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
