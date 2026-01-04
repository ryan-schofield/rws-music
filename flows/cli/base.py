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


class LogCapturingHandler(logging.Handler):
    """Custom logging handler that captures log records."""
    
    def __init__(self):
        super().__init__()
        self.records = []
    
    def emit(self, record):
        """Capture log record."""
        try:
            msg = self.format(record)
            self.records.append(msg)
        except Exception:
            self.handleError(record)


# Create global capturing handler and attach it before basicConfig
_global_log_handler = LogCapturingHandler()
_global_log_handler.setLevel(logging.INFO)  # Capture all levels
_global_log_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
))
logging.getLogger().addHandler(_global_log_handler)
logging.getLogger().setLevel(logging.DEBUG)  # Root logger captures all levels

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
        self.log_handler = _global_log_handler

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
                
                # Add captured logs to result
                result["logs"] = self.log_handler.records
                
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
                        "logs": self.log_handler.records,
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
            "logs": _global_log_handler.records,
        }

    @staticmethod
    def error_result(message: str, errors: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create an error result."""
        return {
            "status": "error",
            "message": message,
            "data": None,
            "errors": errors or [],
            "logs": _global_log_handler.records,
        }

    @staticmethod
    def no_updates_result(message: str) -> Dict[str, Any]:
        """Create a no-updates result."""
        return {
            "status": "no_updates",
            "message": message,
            "data": None,
            "errors": None,
            "logs": _global_log_handler.records,
        }
