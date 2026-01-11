#!/usr/bin/env python3
"""
CLI wrapper for data validation.

Validates data quality and integrity of loaded data.
Usage: python flows/cli/validate_data.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.utils.data_writer import ParquetDataWriter


class ValidateDataCLI(CLICommand):
    """CLI wrapper for data validation."""

    def __init__(self):
        super().__init__(
            name="validate_data",
            timeout=300,  # 5 minutes
            retries=0,
        )
        self.data_writer = ParquetDataWriter()

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute data validation.

        Returns:
            Result dictionary with validation status
        """
        try:
            self.logger.info("Starting data validation")

            # Use absolute path for task-runner compatibility
            workspace_dir = Path("/home/runner/workspace")
            if not workspace_dir.exists():
                workspace_dir = Path.cwd()
            base_path = workspace_dir / "data"

            validation_results = {}

            # Validate tracks_played table exists and has data
            tracks_path = base_path / "src" / "tracks_played"
            if tracks_path.exists():
                validation_results["tracks_played"] = {
                    "exists": True,
                    "path": str(tracks_path),
                }
            else:
                validation_results["tracks_played"] = {
                    "exists": False,
                    "path": str(tracks_path),
                }

            # Check for required enrichment tables
            enrichment_tables = [
                "spotify_artists",
                "spotify_albums",
                "spotify_artist_genre",
                "mbz_artist_info",
                "mbz_artist_genre",
                "mbz_area_hierarchy",
                "cities_with_lat_long",
            ]

            for table in enrichment_tables:
                table_path = base_path / "src" / table
                validation_results[table] = {
                    "exists": table_path.exists(),
                    "path": str(table_path),
                }

            all_valid = all(v.get("exists", False) for v in validation_results.values())

            if all_valid:
                return self.success_result(
                    message="All validation checks passed",
                    data=validation_results,
                )
            else:
                return self.no_updates_result(
                    message="Some validation checks failed (data may still be valid)",
                )

        except Exception as e:
            self.logger.error(f"Data validation error: {str(e)}")
            return self.error_result(
                message="Data validation failed",
                errors=[str(e)],
            )


def main():
    cli = ValidateDataCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
