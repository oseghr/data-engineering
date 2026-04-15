import requests, pathlib, logging

URL = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/UNICEF,NUTRITION,1.0/\
.NT_BF_EXBF+NT_BF_EIBF....?format=csv&startPeriod=2000&endPeriod=2023"

OUTPUT = pathlib.Path("data/raw_breastfeeding.csv")

def download():
    OUTPUT.parent.mkdir(exist_ok=True)
    logging.info("Downloading UNICEF breastfeeding data...")
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    OUTPUT.write_bytes(r.content)
    logging.info(f"Saved {OUTPUT.stat().st_size / 1024:.1f} KB")
    return str(OUTPUT)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    download()