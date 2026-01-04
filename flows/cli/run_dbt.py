#!/usr/bin/env python3
"""
CLI wrapper for dbt transformations.

Runs dbt build (default) or dbt run to transform raw data into reporting-ready tables.
dbt build: Runs tests, freshness checks, and models in the correct order (recommended)
dbt run: Runs only models without tests or freshness checks

Usage: python flows/cli/run_dbt.py --select models/reporting
       python flows/cli/run_dbt.py --command run --select models/reporting
"""

import sys
import argparse
import subprocess
import json
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand


class RunDBTCLI(CLICommand):
    """CLI wrapper for dbt transformations using dbt build."""

    def __init__(self):
        super().__init__(
            name="run_dbt",
            timeout=1200,  # 20 minutes
            retries=2,
        )
        # Use absolute path for task-runner compatibility
        workspace_dir = Path("/home/runner/workspace")
        if not workspace_dir.exists():
            workspace_dir = Path.cwd()
        self.dbt_dir = workspace_dir / "dbt"

    def execute(
        self, 
        select: str = None, 
        exclude: str = None,
        full_refresh: bool = False,
        command: str = "build",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute dbt transformations using dbt build (default) or dbt run.
        
        dbt build runs tests, freshness checks, and models in the correct order,
        providing comprehensive transformation and validation.
        
        Args:
            select: dbt selector for models to run
            exclude: dbt models to exclude
            full_refresh: Force full refresh of all models
            command: dbt command to run ('build' or 'run', default: 'build')
            
        Returns:
            Result dictionary with status and metrics
        """
        try:
            self.logger.info("Starting dbt transformations")
            self.logger.info(f"dbt directory: {self.dbt_dir}")
            self.logger.info(f"dbt command: {command}")
            
            if not self.dbt_dir.exists():
                raise FileNotFoundError(f"dbt directory not found: {self.dbt_dir}")
            
            # Validate command
            if command not in ("build", "run"):
                raise ValueError(f"Invalid dbt command: {command}. Must be 'build' or 'run'")
            
            # Build dbt command
            cmd = [sys.executable, "-m", "dbt.cli", command]
            
            if select:
                cmd.extend(["--select", select])
            if exclude:
                cmd.extend(["--exclude", exclude])
            if full_refresh:
                cmd.append("--full-refresh")
            
            # Change to dbt directory and run
            result = subprocess.run(
                cmd,
                cwd=str(self.dbt_dir),
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
            
            # Parse dbt output for metrics
            output = result.stdout + result.stderr
            self.logger.info("dbt output:")
            for line in output.split("\n"):
                if line.strip():
                    self.logger.info(line)
            
            if result.returncode == 0:
                return self.success_result(
                    message="dbt transformations completed successfully",
                    data={
                        "returncode": result.returncode,
                        "output": output[-1000:] if len(output) > 1000 else output,  # Last 1000 chars
                    },
                )
            else:
                return self.error_result(
                    message="dbt transformations failed",
                    errors=[output],
                )
        
        except subprocess.TimeoutExpired:
            self.logger.error("dbt transformations timed out")
            return self.error_result(
                message="dbt transformations timed out",
                errors=[f"Timeout after {self.timeout} seconds"],
            )
        
        except Exception as e:
            self.logger.error(f"dbt transformations error: {str(e)}")
            return self.error_result(
                message="dbt transformations failed",
                errors=[str(e)],
            )


def main():
    parser = argparse.ArgumentParser(description="Run dbt transformations")
    parser.add_argument(
        "--select",
        type=str,
        default=None,
        help="dbt selector for models to run",
    )
    parser.add_argument(
        "--exclude",
        type=str,
        default=None,
        help="dbt models to exclude",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Force full refresh of all models",
    )
    parser.add_argument(
        "--command",
        type=str,
        default="build",
        choices=["build", "run"],
        help="dbt command to run (default: build)",
    )
    
    args = parser.parse_args()
    
    cli = RunDBTCLI()
    exit_code = cli.run(
        select=args.select,
        exclude=args.exclude,
        full_refresh=args.full_refresh,
        command=args.command,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
