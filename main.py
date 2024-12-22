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
        name="dim_observations",
        write_disposition="replace",
        primary_key=["observation_id"]
    )
    def get_observations() -> Iterator[Dict[str, Any]]:
        metadata_df = get_metadata()

        metadata_df['observation_id'] = metadata_df['cruise'] + '_' + metadata_df['station']

        observations_df = metadata_df.groupby('observation_id').agg({
            'cruise': 'first',
            'station': 'first',
            'year': 'first',
            'month': 'first',
            'day': 'first',
            'hour': 'first',
            'minute': 'first',
            'lat_deg': 'first',
            'lat_min': 'first',
            'lat_dir': 'first',
            'lon_deg': 'first',
            'lon_min': 'first',
            'lon_dir': 'first'
        }).reset_index()

        observations_df['observed_at'] = pd.to_datetime(
            observations_df[['year', 'month', 'day', 'hour', 'minute']]
        )

        observations_df['latitude'] = observations_df.apply(
            lambda x: x['lat_deg'] + x['lat_min']/60 * (1 if x['lat_dir']=='N' else -1),
            axis=1
        )
        observations_df['longitude'] = observations_df.apply(
            lambda x: x['lon_deg'] + x['lon_min']/60 * (1 if x['lon_dir']=='E' else -1),
            axis=1
        )

        observations_df = observations_df[[
            'observation_id',
            'station',
            'observed_at',
            'latitude', 'longitude'
        ]]
        observations_df = observations_df.rename(columns={'station': 'station_id'})

        for obs in observations_df.to_dict('records'):
            yield obs

    def get_metadata() -> pd.DataFrame:
        metadata_str = extract_data(page_url, data_type="metadata")
        metadata_io = io.StringIO(metadata_str)

        return pd.read_csv(
            metadata_io,
            sep='\s+',
            header=None,
            names=[
                'cruise', 'station', '_station', 'year', 'month', 'day',
                'hour', 'minute', 'lat_deg', 'lat_min', 'lat_dir',
                'lon_deg', 'lon_min', 'lon_dir'
            ]
        )

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
