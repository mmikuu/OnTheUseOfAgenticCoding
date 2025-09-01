import pandas as pd
import requests
import time
import os
from tqdm import tqdm

# --- Configuration ---
API_BASE_URL = "https://api.github.com"
# Path to the file containing the GitHub access token
# Please change it if necessary
SETTINGS_FILE = "../../setting.txt"
# Input file name
INPUT_FILE = '../../Data/require_revision/HPR-no-require-revision.csv'
# Output file name
OUTPUT_FILE = '../../Data/require_revision/HPR-no-require-revision.csv'


def load_github_token(settings_file):
    """Loads the GitHub access token from a file."""
    if not os.path.exists(settings_file):
        print(f"Warning: Token file '{settings_file}' not found.")
        print("You may hit the API rate limit. Continuing without authentication.")
        return None
    try:
        with open(settings_file, 'r', encoding='utf-8') as f:
            return f.readline().strip()
    except Exception as e:
        print(f"An error occurred while reading the token file: {e}")
        return None


def add_pr_descriptions(input_path, output_path, token):
    """
    Fetches the description for each PR in the CSV file and adds it as a new column.
    """
    try:
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        print(f"Error: Input file '{input_path}' not found.")
        return

    if token:
        headers = {
            "Authorization": f"bearer {token}",
            "Accept": "application/vnd.github.v3+json"
        }
    else:
        headers = {}

    descriptions = []
    print(f"Loading PR information from '{input_path}' and fetching descriptions...")

    # Visualize progress using tqdm
    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Fetching Descriptions"):
        owner = row['owner']
        repo = row['repo_name']
        pr_number = row['pull_number']

        # Construct the GitHub API endpoint URL
        api_url = f"{API_BASE_URL}/repos/{owner}/{repo}/pulls/{pr_number}"

        try:
            res = requests.get(api_url, headers=headers, timeout=30)
            res.raise_for_status()  # Raise an exception for status codes other than 2xx
            pr_data = res.json()

            # The body (description) of the PR is stored in the 'body' key
            # The body can be empty (None), so store an empty string in that case
            description = pr_data.get('body', '')
            descriptions.append(description)

        except requests.exceptions.RequestException as e:
            print(f"\nError: Failed to fetch PR {owner}/{repo}#{pr_number}: {e}")
            # If an error occurs, record a None value in the list
            descriptions.append(None)

        # Short wait to avoid hitting the API rate limit
        time.sleep(0.1)

    # Add a new 'text' column to the DataFrame
    df['text'] = descriptions

    # Save the results to a new CSV file
    try:
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\nProcessing complete. Results have been saved to '{output_path}'.")
        print(df.head())
    except Exception as e:
        print(f"\nAn error occurred while saving the file: {e}")


# --- Main processing ---
if __name__ == "__main__":
    github_token = load_github_token(SETTINGS_FILE)
    add_pr_descriptions(INPUT_FILE, OUTPUT_FILE, github_token)