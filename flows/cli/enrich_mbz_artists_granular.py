#!/usr/bin/env python3
"""
CLI commands for granular MusicBrainz artist enrichment tasks.

These commands break down the enrichment process into distinct steps:
1. Identify missing MBZ artists (DuckDB query)
2. Fetch artist batch from MusicBrainz API
3. Track failed lookups

This design is optimized for n8n workflows with better resumability and memory efficiency.
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
from flows.enrich.musicbrainz_processor import MusicBrainzProcessor


class IdentifyMissingMBZArtistsCLI(CLICommand):
    """Identify artists that need MusicBrainz enrichment and return batch plan."""

    def __init__(self):
        super().__init__(
            name="identify_missing_mbz_artists",
            timeout=300,  # 5 minutes
            retries=2,
        )
        self.duckdb_engine = DuckDBQueryEngine()
        self.batch_size = 10  # Conservative size for MBZ rate limiting

    def execute(
        self, limit: Optional[int] = None, batch_size: Optional[int] = None, **kwargs
    ) -> Dict[str, Any]:
        """
        Identify missing MBZ artists and return batching information.

        Args:
            limit: Maximum total artists to process
            batch_size: Size of each batch (default 10 for rate limiting)

        Returns:
            Result dictionary with batch plan and metadata
        """
        try:
            if batch_size is not None:
                self.batch_size = batch_size

            self.logger.info("Starting MBZ artist identification")

            # Get total count of missing MBZ artists
            total_count = self.duckdb_engine.get_missing_count("mbz_artists")

            if total_count == 0:
                self.logger.info("No artists need MusicBrainz enrichment")
                return self.no_updates_result(
                    message="No artists need MusicBrainz enrichment"
                )

            # Apply limit if specified
            if limit:
                total_count = min(total_count, limit)

            # Calculate number of batches needed
            num_batches = math.ceil(total_count / self.batch_size)

            self.logger.info(
                f"Found {total_count} artists needing MBZ enrichment across {num_batches} batches"
            )

            # Create batch plan
            batch_plan = []
            for batch_index in range(num_batches):
                offset = batch_index * self.batch_size
                batch_plan.append(
                    {
                        "batch_index": batch_index,
                        "batch_size": self.batch_size,
                        "offset": offset,
                    }
                )

            return self.success_result(
                message=f"Identified {total_count} artists across {num_batches} batches",
                data={
                    "total_count": total_count,
                    "batch_size": self.batch_size,
                    "num_batches": num_batches,
                    "batches": batch_plan,
                },
            )

        except Exception as e:
            self.logger.error(f"MBZ artist identification error: {str(e)}")
            return self.error_result(
                message="Failed to identify missing MBZ artists",
                errors=[str(e)],
            )


class FetchMBZArtistBatchCLI(CLICommand):
    """Fetch a batch of artist data from MusicBrainz API."""

    def __init__(self):
        super().__init__(
            name="fetch_mbz_artist_batch",
            timeout=600,  # 10 minutes
            retries=2,
        )
        self.duckdb_engine = DuckDBQueryEngine()
        self.processor = MusicBrainzProcessor()

    def execute(
        self,
        batch_index: int = 0,
        batch_size: int = 10,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Fetch MBZ data for a specific batch of artists.

        For each artist:
        1. Lookup MBID using ISRC
        2. If found, fetch full artist data
        3. Write JSON to cache directory
        4. Track failures

        Args:
            batch_index: Index of the batch being processed
            batch_size: Number of artists in this batch
            offset: Starting offset for the batch (calculated from batch_index if not provided)

        Returns:
            Result with fetched count and failures
        """
        try:
            if offset is None:
                offset = batch_index * batch_size

            self.logger.info(
                f"Starting MBZ fetch for batch {batch_index} (offset={offset}, size={batch_size})"
            )

            # Get batch of artists from DuckDB
            artists_df = self.duckdb_engine.get_mbz_artists_batch(batch_size, offset)

            if artists_df.is_empty():
                self.logger.info("No artists in batch")
                return self.no_updates_result(
                    message=f"No artists found for batch {batch_index}"
                )

            self.logger.info(f"Processing {len(artists_df)} artists in batch")

            # Process each artist
            fetched = 0
            failed_artists = []

            for row in artists_df.iter_rows(named=True):
                try:
                    artist_id = row["artist_id"]
                    artist_name = row["artist"]
                    isrc = row.get("track_isrc")

                    self.logger.debug(f"Processing artist: {artist_name} ({artist_id})")

                    # Attempt MBZ lookup using ISRC
                    result = self.processor.fetch_artist_by_isrc(
                        isrc, artist_id, artist_name
                    )

                    if result.get("status") == "success":
                        fetched += 1
                        self.logger.debug(
                            f"Successfully fetched MBZ data for {artist_name}"
                        )
                    else:
                        failed_artists.append(
                            {
                                "artist_id": artist_id,
                                "artist": artist_name,
                                "track_isrc": isrc,
                                "reason": result.get("message", "Unknown error"),
                            }
                        )
                        self.logger.debug(
                            f"Failed to fetch MBZ data for {artist_name}: {result.get('message')}"
                        )

                except Exception as e:
                    failed_artists.append(
                        {
                            "artist_id": row["artist_id"],
                            "artist": row["artist"],
                            "track_isrc": row.get("track_isrc"),
                            "reason": str(e),
                        }
                    )
                    self.logger.error(f"Error processing artist in batch: {str(e)}")

            self.logger.info(
                f"Batch {batch_index} complete: {fetched} fetched, {len(failed_artists)} failed"
            )

            return self.success_result(
                message=f"Processed batch {batch_index}: {fetched} fetched",
                data={
                    "batch_index": batch_index,
                    "fetched_count": fetched,
                    "failed_count": len(failed_artists),
                    "failed_artists": failed_artists,
                },
            )

        except Exception as e:
            self.logger.error(f"MBZ batch fetch error: {str(e)}")
            return self.error_result(
                message=f"Failed to fetch MBZ batch {batch_index}",
                errors=[str(e)],
            )


class TrackMBZFailuresCLI(CLICommand):
    """Track artists that failed MusicBrainz lookup."""

    def __init__(self):
        super().__init__(
            name="track_mbz_failures",
            timeout=300,  # 5 minutes
            retries=2,
        )
        self.processor = MusicBrainzProcessor()

    def execute(self, failed_artists: List[Dict], **kwargs) -> Dict[str, Any]:
        """
        Write failed artist lookups to tracking table.

        Args:
            failed_artists: List of artist dicts that failed lookup

        Returns:
            Result with tracking status
        """
        try:
            if not failed_artists:
                self.logger.info("No failed artists to track")
                return self.no_updates_result(message="No failed artists to track")

            self.logger.info(f"Tracking {len(failed_artists)} failed artists")

            # Write failures to tracking table using processor
            result = self.processor.track_failed_artists(failed_artists)

            if result.get("status") == "success":
                # Include the list of failed artists in the output data
                data = {"summary": result, "failed_artists": failed_artists}
                return self.success_result(
                    message=f"Tracked {len(failed_artists)} failed artists",
                    data=data,
                )
            else:
                return self.error_result(
                    message="Failed to track artists",
                    errors=[result.get("message", "Unknown error")],
                )

        except Exception as e:
            self.logger.error(f"MBZ failure tracking error: {str(e)}")
            return self.error_result(
                message="Failed to track MBZ failures",
                errors=[str(e)],
            )


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Granular MusicBrainz artist enrichment tasks"
    )
    parser.add_argument(
        "task",
        choices=["identify", "fetch", "track"],
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
        default=10,
        help="Batch size",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit total artists to process",
    )

    args = parser.parse_args()

    if args.task == "identify":
        cli = IdentifyMissingMBZArtistsCLI()
        exit_code = cli.run(limit=args.limit, batch_size=args.batch_size)
    elif args.task == "fetch":
        cli = FetchMBZArtistBatchCLI()
        exit_code = cli.run(batch_index=args.batch_index, batch_size=args.batch_size)
    elif args.task == "track":
        # Track task requires failed_artists argument, not suitable for CLI
        print("track task must be called programmatically with failed_artists list")
        exit_code = 1
    else:
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
