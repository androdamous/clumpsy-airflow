from croniter import croniter
import yaml
import fnmatch
from pathlib import Path
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List


def parse_partition_pattern(cron_expr: str, base_time: datetime = None) -> str:
    """
    Parses a cron expression to compute the next scheduled run time,
    then returns the partition date in 'yyyymmdd' format.

    :param cron_expr: A cron expression (e.g., '0 0 * * *' for daily).
    :param base_time: Optional starting time (defaults to current UTC time).
    :return: Partition date string in "yyyymmdd" format.
    """
    if base_time is None:
        base_time = datetime.utcnow()
    cron = croniter(cron_expr, base_time)
    next_run = cron.get_next(datetime)
    partition_date = next_run.strftime("%Y%m%d")
    return partition_date


@dataclass
class Manifest:
    """
    A structured data class representing the manifest metadata.
    """
    table_name: str
    table_qualified_name: str
    # Expected in 'yyyymmdd' format (can also match wildcard)
    path: str
    partition_date: str
    load_status: str            # E.g., 'success' or 'failed'
    load_timestamp: str = field(
        default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    


class ManifestManager:
    """
    Manages the creation, retrieval, and search of manifest files in YAML format.
    """

    def __init__(self, output_path: str):
        """
        :param output_path: The directory where YAML manifest files are stored.
        """
        self.output_path = output_path

    def generate_manifest(self, manifest: Manifest) -> str:
        """
        Creates and writes a Manifest (in YAML) to disk based on the provided Manifest instance.
        Returns the path to the generated file.
        """
        # Filename convention: <table_name>_<partition_date>_manifest.yaml
        manifest_filename = f"{manifest.table_name}_{manifest.partition_date}_manifest.yaml"
        full_path = f"{self.output_path}/{manifest_filename}"

        # Write YAML
        with open(full_path, 'w') as f:
            yaml.dump(asdict(manifest), f, sort_keys=False)

        return full_path

    def load_manifest(self, manifest_file_path: str) -> Manifest:
        """
        Loads a manifest file from disk and returns a Manifest instance.
        """
        with open(manifest_file_path, 'r') as f:
            data = yaml.safe_load(f)
        return Manifest(**data)

    def find_matching_manifests(self, table_name: str, partition_pattern: str) -> List[str]:
        """
        Searches the output_path for manifest files matching a specific
        table name and partition date pattern (e.g., '202504*').

        :param table_name: Name of the table for which we want to find manifests.
        :param partition_pattern: A wildcard pattern (e.g., '202504*') for partition dates.
        :return: A list of full file paths to manifest YAML files that match the pattern.
        """
        path = Path(self.output_path)
        matched_files = []

        # Search for all manifest files of the form <table_name>_<any>_manifest.yaml
        file_pattern = f"{table_name}_*_manifest.yaml"

        for file_path in path.glob(file_pattern):
            # Example file_path.stem: "sales_data_20250413_manifest"
            parts = file_path.stem.split('_')
            # parts -> [<table_name>, <partition_date>, "manifest"]
            if len(parts) >= 3:
                file_partition_date = parts[1]  # e.g., "20250413"
                # Check if partition_date matches the user-supplied pattern
                if fnmatch.fnmatch(file_partition_date, partition_pattern):
                    matched_files.append(str(file_path))

        return matched_files


if __name__ == "__main__":
    print(parse_partition_pattern("0 0 * * 1"))
