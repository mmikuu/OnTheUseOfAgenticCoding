import csv
import requests
import time
import os
from datetime import datetime
from typing import Union

# --- Configuration ---
# Filename to load the API token from
TOKEN_FILE = "../../setting.txt"

GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

# List of CSV files to process
INPUT_CSV_PATHS = [
    "../../data/RQ1-APR-MERGED.csv"
]

# Input CSV column names
INPUT_OWNER_COL = "owner"
INPUT_REPO_COL = "repo_name"
INPUT_PR_NUM_COL = "pull_number"

# New column names to add to the CSV
OUTPUT_CREATED_AT_COL = "created_at"
OUTPUT_MERGED_AT_COL = "merged_at"
OUTPUT_TIME_TO_MERGE_COL = "time_to_merge"
NEW_COLUMNS = [OUTPUT_CREATED_AT_COL, OUTPUT_MERGED_AT_COL, OUTPUT_TIME_TO_MERGE_COL]

# --- GraphQL Query ---
# Query to get the creation and merged timestamps of a PR
PR_TIMES_QUERY = """
query GetPrTimes($owner: String!, $repoName: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      createdAt
      mergedAt
    }
  }
}
"""


# --- Helper Functions ---

def load_token(file_path):
    """Loads the GitHub API token from the first line of setting.txt."""
    try:
        with open(file_path, 'r') as f:
            # Changed from f.read() to f.readline() to get only the first line
            token = f.readline().strip()
            if not token:
                print(f"Error: The first line of '{file_path}' is empty. Please write the token in the file.")
                return None
            return token
    except FileNotFoundError:
        print(f"Error: Token file '{file_path}' not found.")
        print("Please create the file in the same directory as the script and write your GitHub PAT inside.")
        return None


def run_graphql_query(query, variables, token):
    """Executes a GitHub GraphQL API query and returns the JSON response."""
    headers = {
        "Authorization": f"bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables}

    try:
        response = requests.post(GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "errors" in data:
            for error in data["errors"]:
                error_type = error.get("type", "UNKNOWN_ERROR")
                error_message = error.get("message", "Unknown error")
                if error_type == "NOT_FOUND":
                    print(f"  GraphQL Error (NOT_FOUND): PR not found.")
                elif error_type == "RATE_LIMITED":
                    print(f"  GraphQL Error (RATE_LIMITED): Rate limit reached. Waiting for a moment.")
                    time.sleep(60)
                else:
                    print(f"  GraphQL Error ({error_type}): {error_message}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        print(f"  Request Error: {e}")
        return None
    except ValueError as e:
        print(f"  JSON Decode Error: {e}")
        return None


def get_pr_times_from_api(owner: str, repo_name: str, pr_number: int, token: str) -> tuple:
    """Gets the creation and merged timestamps for a specific PR."""
    variables = {"owner": owner, "repoName": repo_name, "prNumber": int(pr_number)}
    data = run_graphql_query(PR_TIMES_QUERY, variables, token)
    if data:
        pr_data = data.get("data", {}).get("repository", {}).get("pullRequest", {})
        if pr_data:
            return pr_data.get("createdAt"), pr_data.get("mergedAt")
    return None, None


def calculate_time_to_merge(created_at_str: str, merged_at_str: str) -> Union[str, None]:
    """Calculates the time to merge from timestamp strings."""
    if not created_at_str or not merged_at_str:
        return None
    try:
        created_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        merged_dt = datetime.fromisoformat(merged_at_str.replace('Z', '+00:00'))
        time_delta = merged_dt - created_dt
        return str(time_delta)
    except (ValueError, TypeError):
        return None


# --- Main Processing ---
def main():
    github_token = load_token(TOKEN_FILE)
    if not github_token:
        return

    for file_path in INPUT_CSV_PATHS:
        processed_rows = []
        original_fieldnames = []
        new_fieldnames = []

        print(f"\n--- Starting to process file '{file_path}' ---")

        try:
            with open(file_path, mode='r', encoding='utf-8', newline='') as infile:
                reader = csv.DictReader(infile)
                original_fieldnames = reader.fieldnames if reader.fieldnames else []

                # # Check if all new columns already exist
                # if all(col in original_fieldnames for col in NEW_COLUMNS):
                #     print(f"  Required columns ({', '.join(NEW_COLUMNS)}) already exist. Skipping.")
                #     continue

                new_fieldnames = original_fieldnames + NEW_COLUMNS
                print(f"  Fetching PR timestamp information to add columns ({', '.join(NEW_COLUMNS)}).")

                for row_num, row in enumerate(reader, 1):
                    owner = row.get(INPUT_OWNER_COL, "").strip()
                    repo_name = row.get(INPUT_REPO_COL, "").strip()
                    pr_number_str = row.get(INPUT_PR_NUM_COL, "").strip()

                    # If required information is missing, set error values and continue
                    if not all([owner, repo_name, pr_number_str]):
                        print(f"Warning: Line {row_num}: Missing required information. Skipping.")
                        row[OUTPUT_CREATED_AT_COL] = "ERROR_MISSING_INFO"
                        row[OUTPUT_MERGED_AT_COL] = "ERROR_MISSING_INFO"
                        row[OUTPUT_TIME_TO_MERGE_COL] = "ERROR_MISSING_INFO"
                        processed_rows.append(row)
                        continue

                    try:
                        pr_number = int(pr_number_str)
                    except ValueError:
                        print(f"Warning: Line {row_num}: Invalid PR number '{pr_number_str}'. Skipping.")
                        row[OUTPUT_CREATED_AT_COL] = "ERROR_INVALID_PR_NUM"
                        row[OUTPUT_MERGED_AT_COL] = "ERROR_INVALID_PR_NUM"
                        row[OUTPUT_TIME_TO_MERGE_COL] = "ERROR_INVALID_PR_NUM"
                        processed_rows.append(row)
                        continue

                    print(f"  Fetching: {owner}/{repo_name} PR #{pr_number} (Row {row_num})...")
                    created_at, merged_at = get_pr_times_from_api(owner, repo_name, pr_number, github_token)
                    time_to_merge = calculate_time_to_merge(created_at, merged_at)

                    # Add the fetched information to the row
                    row[OUTPUT_CREATED_AT_COL] = created_at
                    row[OUTPUT_MERGED_AT_COL] = merged_at
                    row[OUTPUT_TIME_TO_MERGE_COL] = time_to_merge
                    processed_rows.append(row)

                    time.sleep(0.1)  # Avoid API rate limit

        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
            continue
        except Exception as e:
            print(f"Error: An unexpected problem occurred while reading file '{file_path}': {e}")
            continue

        # Overwrite the original file with the processed data
        if processed_rows:
            print(f"  Overwriting '{file_path}' with processed data...")
            try:
                with open(file_path, mode='w', encoding='utf-8', newline='') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=new_fieldnames)
                    writer.writeheader()
                    writer.writerows(processed_rows)
                print(f"  Successfully updated '{file_path}'.")
            except Exception as e:
                print(f"Error: A problem occurred while overwriting file '{file_path}': {e}")
        else:
            print(f"  No processable data was found in file '{file_path}'.")


if __name__ == "__main__":
    main()