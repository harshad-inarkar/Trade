import requests
from bs4 import BeautifulSoup
import csv
from  utils.data.paths import HOLIDAYS_LIST_PATH


url = "https://www.nseindia.com/resources/exchange-communication-holidays"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}


with requests.Session() as session:
    # NSE website needs this cookie to not 403
    session.get("https://www.nseindia.com", headers=headers)
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    html = resp.text

soup = BeautifulSoup(html, "html.parser")

# Find the first holidays table (usually called "Trading Holidays")
# The table may have some class, or just be the first table, so let's get all tables & text-search the headers
tables = soup.find_all("table")
holidays = []

for table in tables:
    # Check header row for 'Holiday Date' or 'Date'
    header_row = table.find("tr")
    if header_row and ("Date" in header_row.text or "Holiday Date" in header_row.text):
        # Parse headers
        headers_list = [th.text.strip() for th in header_row.find_all("th")]
        for row in table.find_all("tr")[1:]:
            cells = [td.text.strip() for td in row.find_all("td")]
            # Only take rows matching header size
            if len(cells) == len(headers_list):
                holidays.append(dict(zip(headers_list, cells)))
        break  # Stop after first matching table

if holidays:
    out_fields = holidays[0].keys()
    with open(HOLIDAYS_LIST_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(holidays)
    print(f"Wrote {len(holidays)} holidays to holidays_list.csv")
else:
    print("No holidays table found on the page.")