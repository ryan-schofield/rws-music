#!/usr/bin/env python3
"""
CLI commands for granular geography coordinate enrichment.

Breaks coordinate lookup into batches for better memory management
and progress tracking.

These commands follow the same pattern as MBZ enrichment:
1. Identify cities needing coordinates
2. Fetch coordinates for a batch
3. Write coordinate data to parquet table
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import math

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.utils.duckdb_queries import DuckDBQueryEngine
from flows.enrich.geo_processor import GeographicProcessor


class IdentifyCitiesNeedingCoordinatesCLI(CLICommand):
    """Identify cities that need coordinate lookup."""

    def __init__(self):
        super().__init__(
            name="identify_cities_needing_coordinates",
            timeout=300,  # 5 minutes
            retries=2,
        )
        self.duckdb_engine = DuckDBQueryEngine()
        self.batch_size = 50  # Larger batch size for coordinates (no strict rate limit)

    def execute(self, limit: Optional[int] = None, batch_size: Optional[int] = None, **kwargs) -> Dict[str, Any]:
        """
        Identify cities needing coordinates and return batch plan.

        Uses DuckDB for memory-efficient querying.

        Args:
            limit: Maximum total cities to process
            batch_size: Size of each batch (default 50)

        Returns:
            Result dictionary with batch plan and metadata
        """
        try:
            if batch_size is not None:
                self.batch_size = batch_size

            self.logger.info("Starting city coordinate identification")

            # Get total count of cities needing coordinates
            total_count = self.duckdb_engine.get_missing_count("cities")

            if total_count == 0:
                self.logger.info("No cities need coordinate enrichment")
                return self.no_updates_result(
                    message="No cities need coordinate enrichment"
                )

            # Apply limit if specified
            if limit:
                total_count = min(total_count, limit)

            # Calculate number of batches needed
            num_batches = math.ceil(total_count / self.batch_size)

            self.logger.info(
                f"Found {total_count} cities needing coordinate enrichment across {num_batches} batches"
            )

            # Create batch plan
            batch_plan = []
            for batch_index in range(num_batches):
                offset = batch_index * self.batch_size
                batch_plan.append({
                    "batch_index": batch_index,
                    "batch_size": self.batch_size,
                    "offset": offset,
                })

            return self.success_result(
                message=f"Identified {total_count} cities across {num_batches} batches",
                data={
                    "total_count": total_count,
                    "batch_size": self.batch_size,
                    "num_batches": num_batches,
                    "batches": batch_plan,
                },
            )

        except Exception as e:
            self.logger.error(f"City coordinate identification error: {str(e)}")
            return self.error_result(
                message="Failed to identify cities needing coordinates",
                errors=[str(e)],
            )


class FetchCoordinateBatchCLI(CLICommand):
    """Fetch coordinates for a batch of cities."""

    def __init__(self):
        super().__init__(
            name="fetch_coordinate_batch",
            timeout=600,  # 10 minutes
            retries=2,
        )
        self.duckdb_engine = DuckDBQueryEngine()
        self.processor = GeographicProcessor()

    def execute(
        self,
        batch_index: int = 0,
        batch_size: int = 50,
        offset: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch coordinates from OpenWeather API for a batch.

        Args:
            batch_index: Index of the batch being processed
            batch_size: Number of cities in this batch
            offset: Starting offset for the batch (calculated from batch_index if not provided)

        Returns:
            Result with coordinate data
        """
        try:
            if offset is None:
                offset = batch_index * batch_size

            self.logger.info(
                f"Starting coordinate fetch for batch {batch_index} (offset={offset}, size={batch_size})"
            )

            # Get batch of cities from DuckDB
            cities_df = self.duckdb_engine.get_cities_batch(batch_size, offset)

            if cities_df.is_empty():
                self.logger.info("No cities in batch")
                return self.no_updates_result(
                    message=f"No cities found for batch {batch_index}"
                )

            self.logger.info(f"Processing {len(cities_df)} cities in batch")

            # Process batch using processor
            result = self.processor.enrich_coordinates_batch(
                city_params=[row["params"] for row in cities_df.iter_rows(named=True)]
            )

            if result.get("status") == "success":
                coordinate_count = len(result.get("coordinates", []))
                self.logger.info(
                    f"Batch {batch_index} complete: {coordinate_count} coordinates fetched"
                )

                return self.success_result(
                    message=f"Processed batch {batch_index}: {coordinate_count} coordinates",
                    data={
                        "batch_index": batch_index,
                        "coordinate_count": coordinate_count,
                        "coordinates": result.get("coordinates", []),
                    },
                )
            else:
                return self.error_result(
                    message=f"Failed to fetch coordinates for batch {batch_index}",
                    errors=[result.get("message", "Unknown error")],
                )

        except Exception as e:
            self.logger.error(f"Coordinate batch fetch error: {str(e)}")
            return self.error_result(
                message=f"Failed to fetch coordinate batch {batch_index}",
                errors=[str(e)],
            )


class WriteCoordinateDataCLI(CLICommand):
    """Write coordinate data to parquet table."""

    def __init__(self):
        super().__init__(
            name="write_coordinate_data",
            timeout=300,  # 5 minutes
            retries=2,
        )
        self.processor = GeographicProcessor()

    def execute(self, coordinate_data: List[Dict], **kwargs) -> Dict[str, Any]:
        """
        Write fetched coordinates to cities_with_lat_long table.

        Args:
            coordinate_data: List of coordinate dictionaries to write

        Returns:
            Result with write status
        """
        try:
            if not coordinate_data:
                self.logger.info("No coordinate data to write")
                return self.no_updates_result(
                    message="No coordinate data to write"
                )

            self.logger.info(f"Writing {len(coordinate_data)} coordinates to table")

            result = self.processor.write_coordinates(coordinate_data)

            if result.get("status") == "success":
                return self.success_result(
                    message=f"Wrote {len(coordinate_data)} coordinates",
                    data=result,
                )
            else:
                return self.error_result(
                    message="Failed to write coordinate data",
                    errors=[result.get("message", "Unknown error")],
                )

        except Exception as e:
            self.logger.error(f"Coordinate write error: {str(e)}")
            return self.error_result(
                message="Failed to write coordinate data",
                errors=[str(e)],
            )


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Granular geography coordinate enrichment tasks"
    )
    parser.add_argument(
        "task",
        choices=["identify", "fetch", "write"],
        help="Task to execute",
    )
    parser.add_argument(
        "--batch-index",
        type=int,
        default=0,
        help="Batch index for fetch task",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Batch size",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit total cities to process",
    )

    args = parser.parse_args()

    if args.task == "identify":
        cli = IdentifyCitiesNeedingCoordinatesCLI()
        exit_code = cli.run(limit=args.limit, batch_size=args.batch_size)
    elif args.task == "fetch":
        cli = FetchCoordinateBatchCLI()
        exit_code = cli.run(batch_index=args.batch_index, batch_size=args.batch_size)
    elif args.task == "write":
        # Write task requires coordinate_data argument, not suitable for CLI
        print("write task must be called programmatically with coordinate_data list")
        exit_code = 1
    else:
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
