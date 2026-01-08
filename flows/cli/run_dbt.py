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
import shutil
import os
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
            timeout=2400,  # 40 minutes
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
            
            # Determine how to invoke dbt
            # First check if dbt is in PATH
            dbt_cmd = shutil.which("dbt")
            if not dbt_cmd:
                # Check common installation locations
                common_paths = [
                    Path("/usr/local/bin/dbt"),
                    Path("/opt/runners/task-runner-python/.venv/bin/dbt"),
                    Path("/usr/bin/dbt"),
                ]
                
                found = False
                for dbt_path in common_paths:
                    if dbt_path.exists():
                        dbt_cmd = str(dbt_path)
                        self.logger.info(f"Found dbt at: {dbt_cmd}")
                        found = True
                        break
                
                if not found:
                    # Last resort: try to import and run as Python module
                    try:
                        import dbt
                        dbt_cmd = "dbt"  # Will work via 'python -m dbt'
                        self.logger.info("dbt found as Python module")
                    except ImportError:
                        raise FileNotFoundError(
                            "dbt executable not found in PATH, /usr/local/bin, /opt/runners/task-runner-python/.venv/bin/, "
                            "or as a Python module. Please ensure dbt-core is installed."
                        )
            
            # Build dbt command as a shell string for simpler execution
            # Use 'python -m dbt' as fallback if dbt executable isn't found
            if dbt_cmd and dbt_cmd != "dbt":
                shell_cmd = f"{dbt_cmd} clean && {dbt_cmd} deps"
            else:
                # Use python module syntax which works regardless of installation method
                shell_cmd = f"python -m dbt clean && python -m dbt deps"
            
            # For 'run' command, need to seed first; 'build' does it automatically
            if command == "run":
                if dbt_cmd and dbt_cmd != "dbt":
                    shell_cmd += f" && {dbt_cmd} seed && {dbt_cmd} run"
                else:
                    shell_cmd += " && python -m dbt seed && python -m dbt run"
            else:
                if dbt_cmd and dbt_cmd != "dbt":
                    shell_cmd += f" && {dbt_cmd} {command}"
                else:
                    shell_cmd += f" && python -m dbt {command}"
            
            if select:
                shell_cmd += f" --select {select}"
            if exclude:
                shell_cmd += f" --exclude {exclude}"
            if full_refresh:
                shell_cmd += " --full-refresh"
            
            # Prepare environment for subprocess, ensuring HOME is set for DuckDB
            env = os.environ.copy()
            if not env.get("HOME"):
                env["HOME"] = "/workspace"
            
            # Add environment variables to help with multiprocessing in containers
            env["PYTHONUNBUFFERED"] = "1"
            env["OMP_NUM_THREADS"] = "1"  # Limit OpenMP threads for DuckDB
            
            self.logger.info(f"Running shell command: {shell_cmd}")
            
            # Execute dbt command via shell for simpler environment handling
            result = subprocess.run(
                shell_cmd,
                shell=True,
                cwd=str(self.dbt_dir),
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env=env,
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
                        "output": result.stdout,
                    },
                )
            else:
                return self.error_result(
                    message="dbt transformations failed",
                    errors= result.stdout,
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
