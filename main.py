from dlt.sources.helpers import requests

def main(page_url):
    base_url = "https://gist.githubusercontent.com/Rindrics/"

    response = requests.get(
        base_url + page_url,
        headers={},
    )
    print(response.text)

    return response

if __name__ == "__main__":
    page_url = "689288da5d327f633a6d640ddec27009/raw/97de6556d3798d08f41ae09207077f594ec6a665/gistfile1.txt"
    main(page_url)
