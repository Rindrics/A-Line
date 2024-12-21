from dlt.sources.helpers import requests
from lib.parse import metadata, record
import io
import pandas as pd
import dlt
from typing import Iterator, Dict, Any

@dlt.source
def a_line_source(page_url: str):
    """CTD data of A-Line"""

    @dlt.resource(
        name="metadata",
        write_disposition="replace",
        primary_key=["cruise", "station"]
    )
    def get_metadata() -> Iterator[Dict[str, Any]]:
        metadata_str = extract_data(page_url, data_type="metadata")
        metadata_io = io.StringIO(metadata_str)

        df = pd.read_csv(
            metadata_io,
            sep='\s+',
            header=None,
            names=[
                'cruise', 'station', '_station', 'year', 'month', 'day',
                'hour', 'minute', 'lat_deg', 'lat_min', 'lat_dir',
                'lon_deg', 'lon_min', 'lon_dir'
            ]
        )

        for record in df.to_dict('records'):
            yield record

    @dlt.resource(
        name="records",
        write_disposition="replace",
        primary_key=["cruise", "station", "pressure"]
    )
    def get_records() -> Iterator[Dict[str, Any]]:
        record_str = extract_data(page_url, data_type="record")
        record_io = io.StringIO(record_str)

        df = pd.read_csv(
            record_io,
            sep='\s+',
            header=None,
            names=[
                'cruise', 'station', 'pressure', 'temerature',
                'salinity', 'potential_temp', 'density',
            ]
        )
        for record in df.to_dict('records'):
            yield record

    return [get_metadata, get_records]


def extract_data(page_url: str, data_type: str) -> str:
    base_url = "https://gist.githubusercontent.com/Rindrics/"
    response = requests.get(
        base_url + page_url,
        headers={},
    )
    lines = response.text.splitlines()

    buffer = ""
    for line in lines:
        if not line.strip():
            continue

        if data_type == "metadata":
            parsed = metadata(line)
        else:
            parsed = record(line)

        if parsed:
            buffer += parsed + "\n"

    return buffer

def main():
    pipeline = dlt.pipeline(
        pipeline_name="a_line",
        destination="bigquery",
        dataset_name="ocean_observations",
        progress="log",
    )

    load_info = pipeline.run(
        a_line_source("689288da5d327f633a6d640ddec27009/raw/97de6556d3798d08f41ae09207077f594ec6a665/gistfile1.txt")
    )

    print(load_info)


if __name__ == "__main__":
    main()
