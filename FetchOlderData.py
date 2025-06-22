import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import datetime
from pathlib import Path
import time

# --- Configuration ---

# Base URL and the range of IDs to scrape
BASE_URL = "http://www.puneapmc.org/history.aspx?id=Rates"
START_ID = 4385
END_ID = 4335  # Inclusive

# Output file configuration
OUTPUT_DIR = Path("priceData")
EXCEL_FILENAME = OUTPUT_DIR / f"Pune_Market_Rates_History_{START_ID}_to_{END_ID}.xlsx"

# Static data
MARKET_NAME = "Pune"


def scrape_page_data(url: str, market_name: str) -> pd.DataFrame | None:
    """
    Scrapes all produce data from a single market data page.

    Args:
        url: The URL of the page to scrape.
        market_name: The name of the market (e.g., "Pune").

    Returns:
        A pandas DataFrame containing all data from the page, or None if an error occurs.
    """
    try:
        print(f"Fetching data from {url}...")
        # Use a timeout for the request
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')

    # 1. Extract the date from the H1 tag
    data_date = "Unknown Date"
    h1_tag = soup.find('h2')
    if h1_tag and h1_tag.text:
        match = re.search(r'\((.*?)\)', h1_tag.text)
        if match:
            date_str = match.group(1).strip()
            try:
                # Parse the date and reformat it to DD-MM-YYYY
                data_date = datetime.datetime.strptime(date_str, "%A, %d %b, %Y").strftime("%d-%m-%Y")
            except ValueError:
                print(f"Warning: Could not parse date format. Found: '{date_str}'")
                data_date = date_str  # Keep the original string if parsing fails
    else:
        print("Warning: Could not find the H1 tag containing the date.")

    print(f"  > Found Date: {data_date}")

    # 2. Find all produce sections (h4) and their corresponding tables
    page_dataframes = []
    # Find all h4 tags, which act as headers for data tables
    produce_type_tags = soup.find_all('h4')

    if not produce_type_tags:
        print("Warning: No produce type headers (h4 tags) found on the page.")
        return None

    for h4 in produce_type_tags:
        # Extract the type of produce from the h4 tag
        produce_type = h4.get_text(strip=True).replace("शेतिमालाचा प्रकार -", "").strip()

        # The data table is the next sibling of the h4 tag
        table = h4.find_next_sibling('table')

        if not table:
            print(f"Warning: Found produce type '{produce_type}' but no corresponding table.")
            continue

        # 3. Parse the table into a DataFrame
        try:
            headers = [th.get_text(strip=True) for th in table.find_all('th')]
            rows = [
                [td.get_text(strip=True) for td in row.find_all('td')]
                for row in table.find_all('tr')[1:]  # Skip header row
            ]

            # Ensure rows have the same number of columns as headers
            valid_rows = [row for row in rows if len(row) == len(headers)]

            if not valid_rows:
                continue  # Skip empty tables

            df = pd.DataFrame(valid_rows, columns=headers)

            # 4. Add the new 'Type' column with the value from the h4 tag
            df.insert(0, "शेतिमालाचा प्रकार", produce_type)
            page_dataframes.append(df)

        except Exception as e:
            print(f"Error parsing table for produce type '{produce_type}': {e}")

    if not page_dataframes:
        print("  > No data tables could be successfully parsed from this page.")
        return None

    # 5. Combine all DataFrames from the page and add common columns
    full_page_df = pd.concat(page_dataframes, ignore_index=True)
    full_page_df.insert(0, "Date", data_date)
    full_page_df.insert(1, "Market", market_name)

    print(f"  > Parsed {len(full_page_df)} total rows from the page.")
    return full_page_df


def main():
    """
    Main function to orchestrate the scraping process.
    """
    print("--- Starting Market Data Scraper ---")

    # Generate the list of URLs to scrape
    # We loop from START_ID down to END_ID
    urls_to_scrape = [f"{BASE_URL}{i}" for i in range(START_ID, END_ID - 1, -1)]
    print(f"Preparing to scrape {len(urls_to_scrape)} pages...")

    all_data = []
    for url in urls_to_scrape:
        page_df = scrape_page_data(url, MARKET_NAME)
        if page_df is not None and not page_df.empty:
            all_data.append(page_df)

        # Be a good web citizen and pause between requests
        time.sleep(0.5)

    if not all_data:
        print("--- Script Finished: No data was scraped. ---")
        return

    # Combine all collected dataframes into a single one
    print("\nCombining all scraped data...")
    final_df = pd.concat(all_data, ignore_index=True)

    # Ensure the output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save the final dataframe to a single Excel file
    print(f"Saving all {len(final_df)} rows to '{EXCEL_FILENAME}'...")
    try:
        final_df.to_excel(EXCEL_FILENAME, index=False, engine='openpyxl')
        print("Data saved successfully!")
    except Exception as e:
        print(f"Error saving to Excel file: {e}")

    print("--- Script Finished ---")


if __name__ == "__main__":
    main()
