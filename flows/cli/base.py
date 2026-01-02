"""
Base classes and common functionality for CLI commands.
"""

import sys
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pathlib import Path
import time
from functools import wraps


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class CLICommand(ABC):
    """
    Base class for all CLI commands.
    
    Provides common functionality for error handling, retry logic, JSON output,
    and logging.
    """

    def __init__(self, name: str, timeout: int = 300, retries: int = 0):
        """
        Initialize CLI command.
        
        Args:
            name: Command name for logging
            timeout: Timeout in seconds (default: 300)
            retries: Number of retry attempts on failure (default: 0)
        """
        self.name = name
        self.timeout = timeout
        self.retries = retries
        self.logger = logging.getLogger(f"cli.{name}")

    @abstractmethod
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the command.
        
        Returns:
            Dict with keys: status, message, data, errors
        """
        pass

    def run(self, **kwargs) -> int:
        """
        Run the command with retry logic and error handling.
        
        Returns:
            Exit code (0 for success, 1 for failure)
        """
        attempt = 0
        max_attempts = self.retries + 1
        last_error = None

        while attempt < max_attempts:
            try:
                attempt += 1
                if attempt > 1:
                    self.logger.info(
                        f"Retry attempt {attempt}/{max_attempts} after {60}s delay"
                    )
                    time.sleep(60)

                self.logger.info(f"Starting {self.name} (attempt {attempt}/{max_attempts})")
                
                result = self._execute_with_timeout(**kwargs)
                
                self.logger.info(f"Command completed with status: {result.get('status')}")
                
                # Output JSON result to stdout
                print(json.dumps(result, indent=2))
                
                if result.get("status") == "success":
                    return 0
                else:
                    # Failure but no exception - exit with 1
                    return 1

            except Exception as e:
                last_error = e
                self.logger.error(f"Error on attempt {attempt}: {str(e)}")
                
                if attempt >= max_attempts:
                    # All retries exhausted
                    error_result = {
                        "status": "error",
                        "message": f"Command failed after {max_attempts} attempt(s)",
                        "errors": [str(last_error)],
                        "data": None,
                    }
                    print(json.dumps(error_result, indent=2))
                    return 1

        return 1

    def _execute_with_timeout(self, **kwargs) -> Dict[str, Any]:
        """
        Execute command with timeout enforcement.
        
        Note: Python doesn't have true thread-level timeouts, so this is
        advisory. Tasks should check the timeout and exit gracefully.
        """
        start_time = time.time()
        
        try:
            result = self.execute(**kwargs)
            
            elapsed = time.time() - start_time
            if elapsed > self.timeout:
                self.logger.warning(f"Timeout exceeded: {elapsed:.1f}s > {self.timeout}s")
            
            return result
        
        except Exception as e:
            raise

    @staticmethod
    def success_result(message: str, data: Any = None) -> Dict[str, Any]:
        """Create a success result."""
        return {
            "status": "success",
            "message": message,
            "data": data,
            "errors": None,
        }

    @staticmethod
    def error_result(message: str, errors: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create an error result."""
        return {
            "status": "error",
            "message": message,
            "data": None,
            "errors": errors or [],
        }

    @staticmethod
    def no_updates_result(message: str) -> Dict[str, Any]:
        """Create a no-updates result."""
        return {
            "status": "no_updates",
            "message": message,
            "data": None,
            "errors": None,
        }
