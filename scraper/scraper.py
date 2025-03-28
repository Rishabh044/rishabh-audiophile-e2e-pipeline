import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
from typing import Optional


def scrape_tables(url, table_index=0, output_file=None) -> Optional[pd.DataFrame]:
    """
    Scrape tables from a given URL and return them as pandas DataFrames
    
    Args:
        url (str): The URL of the webpage containing tables
        table_index (int, optional): Index of the table to extract. Defaults to 0 (first table).
        output_file (str, optional): Path to save the CSV file. 
    
    Returns:
        list of pandas.DataFrame: List of DataFrames containing table data
    """
    try:
        # Send a GET request to the webpage
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all tables
        tables = soup.find_all('table')
        
        if not tables:
            raise ValueError("No tables found on the page")

        # Validate table index
        if table_index >= len(tables):
            raise ValueError(f"Table index {table_index} is out of range. Only {len(tables)} tables found.")

        # Select the specific table
        table = tables[table_index]

        # Extract table headers (try multiple methods)
        headers = []
        header_candidates = [
            table.find_all('th'),
            table.find('thead').find_all('td') if table.find('thead') else [],
            table.find_all('tr')[0].find_all('td')
        ]

        for candidate in header_candidates:
            if candidate:
                headers = [header.get_text(strip=True) for header in candidate]
                break

        # Extract table rows
        rows = []
        row_candidates = table.find_all('tr')[1:] if len(headers) else table.find_all('tr')
        
        for row in row_candidates:
            row_data = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
            if row_data:  # Only add non-empty rows
                rows.append(row_data)

        # If no headers were found, generate generic column names
        if not headers:
            headers = [f'Column_{i+1}' for i in range(len(rows[0]) if rows else 0)]

        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers)

        # Optionally save to CSV
        if output_file:
            df.to_csv(output_file, index=False)
            print(f"Table saved to {output_file}")

        return df

    except requests.RequestException as e:
        print(f"Error fetching the webpage: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def main():
    iems_df = scrape_tables("https://crinacle.com/rankings/iems/")
    headphones_df = scrape_tables("https://crinacle.com/rankings/headphones/")

    iems_df.to_csv('./datasets/iems_rankings.csv', index=False)
    headphones_df.to_csv('./datasets/headphones_rankings.csv', index=False)
    print("IEMs and headphones data saved to ../datasets/")

if __name__ == '__main__':
    main()