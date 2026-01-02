#!/usr/bin/env python3
"""
Test script for validating all CLI commands work independently.

This is a manual developer utility for validating that all CLI wrappers
execute successfully with test parameters. Not automated testing, but useful
for pre-deployment verification.

Usage: python flows/cli/test_all_commands.py
"""

import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class CLITester:
    """Test runner for all CLI commands."""

    def __init__(self):
        self.cli_dir = Path(__file__).parent
        self.results: Dict[str, dict] = {}
        self.test_commands = [
            ("validate_data.py", []),
            ("load_raw_tracks.py", []),
            ("ingest_spotify.py", ["--limit", "5"]),
            ("enrich_spotify_artists.py", ["--limit", "5"]),
            ("enrich_spotify_albums.py", ["--limit", "5"]),
            ("discover_mbz_artists.py", []),
            ("fetch_mbz_artists.py", ["--limit", "5"]),
            ("parse_mbz_data.py", []),
            ("process_mbz_hierarchy.py", []),
            ("enrich_geography.py", []),
            ("run_dbt.py", ["--select", "intermediate"]),
            ("update_mbids.py", []),
        ]

    def run_all_tests(self) -> bool:
        """
        Run all CLI tests sequentially.
        
        Returns:
            True if all tests passed, False otherwise
        """
        print("=" * 80)
        print("CLI COMMAND TEST SUITE")
        print("=" * 80)
        print()
        
        passed = 0
        failed = 0
        
        for script, args in self.test_commands:
            print(f"Testing {script}...")
            success = self.test_command(script, args)
            
            if success:
                passed += 1
                print(f"  ✅ PASSED\n")
            else:
                failed += 1
                print(f"  ❌ FAILED\n")
        
        print("=" * 80)
        print(f"RESULTS: {passed} passed, {failed} failed")
        print("=" * 80)
        
        return failed == 0

    def test_command(self, script: str, args: List[str]) -> bool:
        """
        Test a single CLI command.
        
        Args:
            script: Script filename
            args: Additional command line arguments
            
        Returns:
            True if command succeeded, False otherwise
        """
        script_path = self.cli_dir / script
        
        if not script_path.exists():
            print(f"  ⚠️  Script not found: {script_path}")
            self.results[script] = {"status": "error", "message": "Script not found"}
            return False
        
        try:
            cmd = [sys.executable, str(script_path)] + args
            
            print(f"  Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,  # 1 minute timeout for test
                cwd=str(project_root),
            )
            
            # Parse JSON output
            try:
                output = json.loads(result.stdout)
                status = output.get("status", "unknown")
                message = output.get("message", "")
                
                self.results[script] = {
                    "status": status,
                    "message": message,
                    "exit_code": result.returncode,
                }
                
                # Success if exit code is 0, regardless of command status
                success = result.returncode == 0
                
                if not success:
                    print(f"  Message: {message}")
                    if result.stderr:
                        print(f"  Stderr: {result.stderr[:200]}")
                
                return success
            
            except json.JSONDecodeError:
                print(f"  Failed to parse JSON output")
                print(f"  Stdout: {result.stdout[:200]}")
                print(f"  Stderr: {result.stderr[:200]}")
                self.results[script] = {
                    "status": "error",
                    "message": "Failed to parse JSON output",
                }
                return False
        
        except subprocess.TimeoutExpired:
            print(f"  Command timed out after 60 seconds")
            self.results[script] = {
                "status": "timeout",
                "message": "Command timed out",
            }
            return False
        
        except Exception as e:
            print(f"  Exception: {str(e)}")
            self.results[script] = {
                "status": "error",
                "message": str(e),
            }
            return False

    def print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        
        for script, result in self.results.items():
            status_icon = (
                "✅" if result.get("status") == "success" else
                "❌" if result.get("status") == "error" else
                "⚠️"
            )
            print(f"{status_icon} {script}: {result.get('status')} - {result.get('message')}")


def main():
    """Run all tests."""
    tester = CLITester()
    success = tester.run_all_tests()
    tester.print_summary()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
