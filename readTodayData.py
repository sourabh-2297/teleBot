import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re  # Import the 're' module for regular expressions
import datetime
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows

today = str(datetime.date.today())
#9730086599/9697041001#ShivamBhandurge
# Configuration
#URL_List = ["http://www.puneapmc.org/rates.aspx?page=rates&catid=1","http://www.puneapmc.org/rates.aspx?page=rates&catid=2","http://www.puneapmc.org/rates.aspx?page=rates&catid=3","http://www.puneapmc.org/rates.aspx?page=rates&catid=4"]
URL_List = ["http://www.puneapmc.org/history.aspx?id=Rates4385"]
#EXCEL_FILENAME = f"priceData\Pune_market_rates_{today.replace('-', '_')}.xlsx"
EXCEL_FILENAME = f"priceData\Pune_market_rates_Test.xlsx"
MARKET_NAME = "Pune"  # Define the market name

def scrape_and_save_market_data(url, excel_filename, market_name):
    """
    Scrapes market data, extracts date, adds date and market columns, and appends to Excel.
    """
    try:
        print(f"Fetching data from {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print("Data fetched successfully.")

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract date (with robust handling)
        data_date_str = "Unknown Date"
        h1_tag = soup.find('h1')
        if h1_tag and h1_tag.text:
            match = re.search(r'\((.*?)\)', h1_tag.text)
            if match:
                date_str = match.group(1).strip()
                try:
                    data_date = datetime.datetime.strptime(date_str, "%A, %d %b, %Y").strftime("%d-%m-%Y")
                except ValueError:
                    print(f"Could not parse date format. Found: {date_str}")
                    data_date = "Unknown Date"
                print(f"Processed Data date: {data_date}")
            else:
                print("Could not parse date from H1 tag.")
        else:
            print("Could not find the H1 tag containing the date.")

        # Extract table (with fallback)
        table = soup.find('table', id='DG')
        if not table:
            h3_tag = soup.find('h3', text=lambda t: t and 'शेतिमालाचा प्रकार' in t)
            if h3_tag:
                table = h3_tag.find_next_sibling('table')
        if not table:
            print("Could not find the rates table.")
            return None, None

        # Extract headers and rows
        headers = [th.text.strip() for th in table.find_all('th')]
        rows = [[td.text.strip() for td in row.find_all('td')]
                for row in table.find_all('tr')[1:]
                if len(row.find_all('td')) == len(headers)]

        if not rows:
            print("No data rows found.")
            return None, None

        df = pd.DataFrame(rows, columns=headers)
        print(f"Parsed {len(df)} rows.")

        # Add new columns BEFORE saving/appending
        df.insert(0, "Date", data_date)  # Insert at the beginning
        df.insert(1, "Market", market_name)  # Insert market name

        # Append or create Excel (consolidated logic)
        if os.path.exists(excel_filename):
            print(f"Appending to '{excel_filename}'...")
            book = openpyxl.load_workbook(excel_filename)
            sheet = book.active
            for row in dataframe_to_rows(df, index=False, header=False):
                sheet.append(row)
            book.save(excel_filename)
            print("Data appended.")
        else:
            print(f"Creating '{excel_filename}'...")
            df.to_excel(excel_filename, index=False)
            print("File created and data saved.")

        return excel_filename, data_date

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None, None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None


if __name__ == "__main__":
    print("--- Starting Market Data Scraper ---")
    for url in URL_List:
        filepath, date = scrape_and_save_market_data(url, EXCEL_FILENAME, MARKET_NAME)
    print("--- Script Finished ---")