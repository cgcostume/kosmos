#!/usr/bin/env python3
"""
Generic File Operations Module

Provides safe file operations including moving, copying, and batch operations
with cross-drive support, error handling, and progress tracking.
"""

import pathlib
import shutil
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple


class OperationType(Enum):
    """Type of file operation"""

    MOVE = "move"
    COPY = "copy"


@dataclass
class FileOperation:
    """Represents a planned file operation"""

    source_path: pathlib.Path
    target_path: pathlib.Path
    operation_type: OperationType
    identifier: str = ""  # Optional identifier for tracking

    def __post_init__(self):
        if not self.identifier:
            self.identifier = self.source_path.name


@dataclass
class OperationResult:
    """Result of a file operation"""

    operation: FileOperation
    success: bool
    error_message: Optional[str] = None


class FileOperations:
    """Generic file operations handler with cross-drive support"""

    def __init__(self, progress_callback: Optional[Callable[[str], None]] = None):
        """Initialize with optional progress callback"""
        self.progress_callback = progress_callback

    def execute_operation(self, operation: FileOperation) -> OperationResult:
        """Execute a single file operation"""
        try:
            # Create target directory if it doesn't exist
            target_dir = operation.target_path.parent
            target_dir.mkdir(parents=True, exist_ok=True)

            if operation.operation_type == OperationType.COPY:
                # Copy mode - always copy, preserve original
                shutil.copy2(operation.source_path, operation.target_path)

            elif operation.operation_type == OperationType.MOVE:
                # Move mode - try rename first, fallback to copy+delete for cross-drive
                try:
                    operation.source_path.rename(operation.target_path)
                except OSError as rename_error:
                    if self._is_cross_drive_error(rename_error):
                        # Cross-drive operation - use copy + delete
                        shutil.copy2(operation.source_path, operation.target_path)
                        operation.source_path.unlink()  # Delete original after successful copy
                    else:
                        raise  # Re-raise if it's a different error

            return OperationResult(operation=operation, success=True)

        except Exception as e:
            return OperationResult(operation=operation, success=False, error_message=str(e))

    def execute_batch_operations(
        self, operations: list[FileOperation]
    ) -> tuple[list[OperationResult], list[OperationResult]]:
        """Execute multiple file operations and return success/failure lists"""
        if not operations:
            return [], []

        successful_operations = []
        failed_operations = []

        for i, operation in enumerate(operations):
            # Progress callback if provided
            if self.progress_callback:
                self.progress_callback(f"Processing {operation.identifier} ({i + 1}/{len(operations)})")

            result = self.execute_operation(operation)

            if result.success:
                successful_operations.append(result)
            else:
                failed_operations.append(result)

        return successful_operations, failed_operations

    def plan_operation(
        self, source_path: pathlib.Path, target_path: pathlib.Path, operation_type: OperationType, identifier: str = ""
    ) -> FileOperation:
        """Create a planned file operation"""
        return FileOperation(
            source_path=source_path,
            target_path=target_path,
            operation_type=operation_type,
            identifier=identifier or source_path.name,
        )

    def plan_batch_operations(
        self, file_mappings: dict[pathlib.Path, pathlib.Path], operation_type: OperationType
    ) -> list[FileOperation]:
        """Create multiple planned operations from source->target mappings"""
        operations = []
        for source_path, target_path in file_mappings.items():
            operation = self.plan_operation(source_path, target_path, operation_type)
            operations.append(operation)
        return operations

    def _is_cross_drive_error(self, error: OSError) -> bool:
        """Check if the error indicates a cross-drive operation"""
        error_str = str(error).lower()
        return (
            "different disk drive" in error_str
            or error.errno == 17  # Cross-device link error
            or "cross-device link" in error_str
        )

    def safe_move_file(self, source_path: pathlib.Path, target_path: pathlib.Path) -> bool:
        """Safely move a file with cross-drive support"""
        operation = self.plan_operation(source_path, target_path, OperationType.MOVE)
        result = self.execute_operation(operation)
        return result.success

    def safe_copy_file(self, source_path: pathlib.Path, target_path: pathlib.Path) -> bool:
        """Safely copy a file"""
        operation = self.plan_operation(source_path, target_path, OperationType.COPY)
        result = self.execute_operation(operation)
        return result.success
