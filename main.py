from dlt.sources.helpers import requests
from lib.parse import metadata, record
import io
import pandas as pd

def main(page_url):
    base_url = "https://gist.githubusercontent.com/Rindrics/"

    response = requests.get(
        base_url + page_url,
        headers={},
    )
    print(response.text)

    lines = response.text.splitlines()

    metadata_str = ""
    record_str = ""

    def append_line(buffer, line):
        if not line == "":
            return buffer + line + "\n"
        return buffer

    for line in lines:
        # skip if blank line
        if not line.strip():
            continue

        metadata_str = append_line(metadata_str, metadata(line))
        record_str = append_line(record_str, record(line))

    metadata_io = io.StringIO(metadata_str)
    record_io = io.StringIO(record_str)

    df_meta = pd.read_csv(
        metadata_io,
        delim_whitespace=True,
        header=None,
        names=[
            'cruise', 'station', '_station', 'year', 'month', 'day',
            'hour', 'minute', 'lat_deg', 'lat_min', 'lat_dir',
            'lon_deg', 'lon_min', 'lon_dir'
        ]
    )

    record_meta = pd.read_csv(
        record_io,
        delim_whitespace=True,
        header=None,
        names=[
            'cruise', 'station', 'pressure', 'temerature',
            'salinity', 'potential_temp', 'density',
        ]
    )

    print(record_meta)
    return None

if __name__ == "__main__":
    page_url = "689288da5d327f633a6d640ddec27009/raw/97de6556d3798d08f41ae09207077f594ec6a665/gistfile1.txt"
    main(page_url)
