from dlt.sources.helpers.rest_client import RESTClient
import requests

def main(session):
    client = RESTClient(
        base_url="https://gist.githubusercontent.com/Rindrics/",
        headers={"User-Agent": "MyApp/1.0"},
        data_selector="data",
        session=session,
    )
    response = client.get("689288da5d327f633a6d640ddec27009/raw/97de6556d3798d08f41ae09207077f594ec6a665/gistfile1.txt")
    print(response.content)
    return response

if __name__ == "__main__":
    session = requests.Session()
    main(session)
