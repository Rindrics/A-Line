from dlt.sources.helpers import requests
from lib.parse import metadata, record

def main(page_url, file_metadata, file_record):
    base_url = "https://gist.githubusercontent.com/Rindrics/"

    response = requests.get(
        base_url + page_url,
        headers={},
    )
    print(response.text)

    lines = response.text.splitlines()

    with open(file_metadata, 'a') as meta_file, open(file_record, 'a') as rec_file:
        for line in lines:

            # skip if blank line
            if not line.strip():
                continue

            result_metadata = metadata(line)
            if not result_metadata == "":
                meta_file.write(f"{result_metadata}\n")

            result_record = record(line)
            if not result_record == "":
                rec_file.write(f"{result_record}\n")

    return None

if __name__ == "__main__":
    page_url = "689288da5d327f633a6d640ddec27009/raw/97de6556d3798d08f41ae09207077f594ec6a665/gistfile1.txt"
    main(page_url, "metadata_test.txt", "record_test.txt")
