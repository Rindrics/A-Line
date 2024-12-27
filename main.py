from dlt.sources.helpers import requests
from lib.parse import metadata, record
import io
import pandas as pd
import dlt
from typing import Iterator, Dict, Any
import hashlib

A_LINE_STATIONS = [
    # ref: https://ocean.fra.go.jp/a-line/a-line_research.html
    {'station_id': 'A01', 'lat_deg': 42, 'lat_min': 50.0, 'lon_deg': 144, 'lon_min': 50.0, 'depth': 99},
    {'station_id': 'A02', 'lat_deg': 42, 'lat_min': 40.0, 'lon_deg': 144, 'lon_min': 55.0, 'depth': 400},
    {'station_id': 'A25', 'lat_deg': 42, 'lat_min': 35.0, 'lon_deg': 144, 'lon_min': 57.5, 'depth': 1200},
    {'station_id': 'A03', 'lat_deg': 42, 'lat_min': 30.0, 'lon_deg': 145, 'lon_min': 00.0, 'depth': 1780},
    {'station_id': 'A35', 'lat_deg': 42, 'lat_min': 21.0, 'lon_deg': 145, 'lon_min': 04.5, 'depth': 2974},
    {'station_id': 'A04', 'lat_deg': 42, 'lat_min': 15.0, 'lon_deg': 145, 'lon_min': 07.5, 'depth': 2950},
    {'station_id': 'A45', 'lat_deg': 42, 'lat_min': 07.0, 'lon_deg': 145, 'lon_min': 11.3, 'depth': 3200},
    {'station_id': 'A05', 'lat_deg': 42, 'lat_min': 00.0, 'lon_deg': 145, 'lon_min': 15.0, 'depth': 4000},
    {'station_id': 'A55', 'lat_deg': 41, 'lat_min': 52.5, 'lon_deg': 145, 'lon_min': 18.8, 'depth': 4500},
    {'station_id': 'A06', 'lat_deg': 41, 'lat_min': 45.0, 'lon_deg': 145, 'lon_min': 22.5, 'depth': 5280},
    {'station_id': 'A07', 'lat_deg': 41, 'lat_min': 30.0, 'lon_deg': 145, 'lon_min': 30.0, 'depth': 7150},
    {'station_id': 'A08', 'lat_deg': 41, 'lat_min': 15.0, 'lon_deg': 145, 'lon_min': 37.5, 'depth': 6320},
    {'station_id': 'A09', 'lat_deg': 41, 'lat_min': 00.0, 'lon_deg': 145, 'lon_min': 45.0, 'depth': 5580},
    {'station_id': 'A10', 'lat_deg': 40, 'lat_min': 45.0, 'lon_deg': 145, 'lon_min': 52.5, 'depth': 5280},
    {'station_id': 'A11', 'lat_deg': 40, 'lat_min': 30.0, 'lon_deg': 146, 'lon_min': 00.0, 'depth': 5160},
    {'station_id': 'A12', 'lat_deg': 40, 'lat_min': 15.0, 'lon_deg': 146, 'lon_min': 07.5, 'depth': 5250},
    {'station_id': 'A13', 'lat_deg': 40, 'lat_min': 00.0, 'lon_deg': 146, 'lon_min': 15.0, 'depth': 4900},
    {'station_id': 'A14', 'lat_deg': 39, 'lat_min': 45.0, 'lon_deg': 146, 'lon_min': 22.5, 'depth': 5170},
    {'station_id': 'A15', 'lat_deg': 39, 'lat_min': 30.0, 'lon_deg': 146, 'lon_min': 30.0, 'depth': 5200},
    {'station_id': 'A16', 'lat_deg': 39, 'lat_min': 15.0, 'lon_deg': 146, 'lon_min': 37.5, 'depth': 5210},
    {'station_id': 'A17', 'lat_deg': 39, 'lat_min': 00.0, 'lon_deg': 146, 'lon_min': 45.0, 'depth': 5200},
    {'station_id': 'A18', 'lat_deg': 38, 'lat_min': 45.0, 'lon_deg': 146, 'lon_min': 52.5, 'depth': 5200},
    {'station_id': 'A19', 'lat_deg': 38, 'lat_min': 30.0, 'lon_deg': 147, 'lon_min': 00.0, 'depth': 5200},
    {'station_id': 'A20', 'lat_deg': 38, 'lat_min': 15.0, 'lon_deg': 147, 'lon_min': 07.5, 'depth': 5200},
    {'station_id': 'A21', 'lat_deg': 38, 'lat_min': 00.0, 'lon_deg': 147, 'lon_min': 15.0, 'depth': 5200},
]

@dlt.source
def a_line_source(page_url: str):
    """CTD data of A-Line"""

    @dlt.resource(
        name="dim_stations",
        write_disposition="replace",
        primary_key=["station_id"]
    )
    def get_stations() -> Iterator[Dict[str, Any]]:
        for station in A_LINE_STATIONS:
            yield station

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

    @dlt.resource(
        name="fact_profiles",
        write_disposition="replace",
    )
    def get_profiles() -> Iterator[Dict[str, Any]]:
        profile_str = extract_data(page_url, data_type="profile")
        profile_io = io.StringIO(profile_str)

        profile_df = pd.read_csv(
            profile_io,
            sep='\s+',
            header=None,
            names=[
                'cruise', 'station', 'pressure', 'temperature',
                'salinity', 'potential_temp', 'density',
            ]
        )

        profile_df['observation_id'] = profile_df['cruise'] + '_' + profile_df['station']

        records = profile_df.to_dict('records')
        for profile in records:
            # create primary key
            hash_str = f"{profile['observation_id']}_{profile['pressure']}"
            profile['profile_id'] = hashlib.md5(hash_str.encode()).hexdigest()
            yield profile

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

    return [get_stations, get_observations, get_profiles]


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
