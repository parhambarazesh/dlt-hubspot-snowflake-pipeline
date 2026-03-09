import gzip
from collections import defaultdict
from typing import Any, Dict, List

import dlt

from hubspot import hubspot, hubspot_events_for_objects, THubspotObjectType


def _count_jsonl_rows(file_path: str, is_compressed: bool) -> int:
    """Count rows in a dlt job data file (jsonl/jsonl.gz)."""
    line_count = 0
    open_fn = gzip.open if is_compressed else open
    with open_fn(file_path, "rt", encoding="utf-8") as f:
        for _ in f:
            line_count += 1
    return line_count


def _log_rows_sent_to_snowflake(pipeline: dlt.Pipeline, load_info: Any) -> None:
    """Print per-table row counts for the current run's load packages."""
    info_dict = load_info.asdict() if hasattr(load_info, "asdict") else {}
    load_ids = info_dict.get("loads_ids", [])
    if not load_ids:
        print("[ROW LOG] No load packages created. Rows sent to Snowflake: 0")
        return

    rows_by_table: Dict[str, int] = defaultdict(int)
    total_rows = 0

    for load_id in load_ids:
        package_info = pipeline.get_load_package_info(load_id).asdict()
        for job in package_info.get("jobs", []):
            table_name = job.get("table_name")
            if not table_name or table_name.startswith("_dlt"):
                continue
            if job.get("file_format") != "jsonl":
                continue

            file_path = job.get("file_path")
            if not file_path:
                continue

            try:
                row_count = _count_jsonl_rows(
                    file_path=file_path,
                    is_compressed=bool(job.get("is_compressed")),
                )
            except OSError as ex:
                print(f"[ROW LOG] Could not read job file for {table_name}: {ex}")
                continue

            rows_by_table[table_name] += row_count
            total_rows += row_count

    print(f"[ROW LOG] Total rows sent to Snowflake in this run: {total_rows}")
    if rows_by_table:
        for table_name, row_count in sorted(rows_by_table.items()):
            print(f"[ROW LOG] {table_name}: {row_count}")
    else:
        print("[ROW LOG] No table data files detected for this run.")


def load_crm_data() -> None:
    """
    This function loads all resources from HubSpot CRM

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    # Add full_refresh=(True or False) if you need your pipeline to create the dataset in your destination
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination='snowflake',
    )

    # Run the pipeline with the HubSpot source connector
    info = p.run(hubspot(), write_disposition="merge")

    # Print information about the pipeline run
    print(info)
    _log_rows_sent_to_snowflake(p, info)


def load_crm_data_with_history() -> None:
    """
    Loads all HubSpot CRM resources and property change history for each entity.
    The history entries are loaded to a tables per resource `{resource_name}_property_history`, e.g. `contacts_property_history`

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    # Add full_refresh=(True or False) if you need your pipeline to create the dataset in your destination
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination='snowflake',
    )

    # Configure the source with `include_history` to enable property history load, history is disabled by default
    data = hubspot(include_history=True)

    # Run the pipeline with the HubSpot source connector
    info = p.run(data)

    # Print information about the pipeline run
    print(info)
    _log_rows_sent_to_snowflake(p, info)


def load_crm_data_with_soft_delete() -> None:
    """
    Loads all HubSpot CRM resources, including soft-deleted (archived) records for each entity.
    By default, only the current state of the records is loaded; property change history is not included unless explicitly enabled.

    Soft-deleted records are retrieved and marked appropriately, allowing both active and archived data to be processed.
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type.
    # You can add `full_refresh=True` if the pipeline should recreate the dataset at the destination.
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination='snowflake',
    )

    # Configure the source to load soft-deleted (archived) records.
    # Property change history is disabled by default unless configured separately.
    data = hubspot(soft_delete=True)

    # Run the pipeline with the HubSpot source connector.
    info = p.run(data, write_disposition="merge", full_refresh=False,primary_key="hs_object_id")

    # Print information about the pipeline run.
    print(info)
    _log_rows_sent_to_snowflake(p, info)


def load_crm_objects_with_custom_properties() -> None:
    """
    Loads CRM objects, reading only properties defined by the user.
    """

    # Create a DLT pipeline object with the pipeline name,
    # dataset name, properties to read and destination database
    # type Add full_refresh=(True or False) if you need your
    # pipeline to create the dataset in your destination
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination='snowflake',
    )

    load_data = hubspot(
        properties={"contact": ["date_of_birth", "degree"]}, include_custom_props=True
    )
    load_info = pipeline.run(load_data)
    print(load_info)
    _log_rows_sent_to_snowflake(pipeline, load_info)


def load_pipelines() -> None:
    """
    This function loads web analytics events for a list objects in `object_ids` of type `object_type`

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination='snowflake',
        dev_mode=False,
    )
    # To load data from pipelines in "deals" endpoint
    load_data = hubspot().with_resources("pipelines_deals", "stages_timing_deals")
    load_info = p.run(load_data)
    print(load_info)
    _log_rows_sent_to_snowflake(p, load_info)


def load_web_analytics_events(
    object_type: THubspotObjectType, object_ids: List[str]
) -> None:
    """
    This function loads web analytics events for a list objects in `object_ids` of type `object_type`

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination='snowflake',
        dev_mode=False,
    )

    # you can get many resources by calling this function for various object types
    resource = hubspot_events_for_objects(object_type, object_ids)
    # and load them together passing resources in the list
    info = p.run([resource])

    # Print information about the pipeline run
    print(info)
    _log_rows_sent_to_snowflake(p, info)


def load_selected_crm_data(datatype) -> None:
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination='snowflake',
    )

    data = hubspot().with_resources(*datatype)

    info = p.run(data, write_disposition="merge")
    print(info)
    _log_rows_sent_to_snowflake(p, info)


if __name__ == "__main__":
    load_selected_crm_data(["contacts", "companies"])
    # load_crm_data()
    # load_crm_data_with_history()
    # load_crm_objects_with_custom_properties()
    # load_pipelines()
    # load_crm_data_with_soft_delete()
    # load_web_analytics_events("company", ["7086461639", "7086464459"])
