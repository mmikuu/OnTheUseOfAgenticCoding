import csv
import requests
import time
import os

# This script reads pull request data from a CSV file.
# It then uses the GitHub GraphQL API to fetch the current state for each PR and adds it as a new column, overwriting the original file.

# --- Configuration ---
# It is strongly recommended to get the GitHub Personal Access Token (PAT) from a file or environment variable.
SETTINGS_FILE = "../../setting.txt"
GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

# List of CSV files to process
INPUT_CSV_PATHS = [
    "../../data/commit_details/HPR-commit-details.csv",
]

# Input CSV column names
INPUT_OWNER_COL = "owner"
INPUT_REPO_COL = "repo_name"
INPUT_PR_NUM_COL = "pull_number"
OUTPUT_AUTHOR_COL = "state"  # Name of the column to add

# --- GraphQL Query ---
PR_AUTHOR_QUERY = """
query GetPrAuthor($owner: String!, $repoName: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      state
    }
  }
}
"""


# --- Helper Functions ---

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


def run_graphql_query(query, variables, token):
    """Executes a GitHub GraphQL API query and returns the JSON response."""
    headers = {
        "Authorization": f"bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "query": query,
        "variables": variables
    }

    try:
        response = requests.post(GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=30)
        response.raise_for_status()  # Check for HTTP errors (4xx, 5xx)
        data = response.json()
        if "errors" in data:
            for error in data["errors"]:
                error_type = error.get("type", "UNKNOWN_ERROR")
                error_message = error.get("message", "Unknown error")
                if error_type == "NOT_FOUND":
                    print(f"  GraphQL Error (NOT_FOUND): PR not found.")
                elif error_type == "FORBIDDEN":
                    print(f"  GraphQL Error (FORBIDDEN): Insufficient permissions. Please check the token scopes.")
                elif error_type == "RATE_LIMITED":
                    print(f"  GraphQL Error (RATE_LIMITED): Rate limit reached. Waiting for a moment.")
                    time.sleep(60)  # Wait for 1 minute
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


def get_pr_author_from_api(owner, repo_name, pr_number, token):
    """Gets the author login name for a specific PR."""
    variables = {
        "owner": owner,
        "repoName": repo_name,
        "prNumber": int(pr_number)
    }
    data = run_graphql_query(PR_AUTHOR_QUERY, variables, token)
    if data:
        pr_state = data.get("data", {}).get("repository", {}).get("pullRequest", {}).get("state")
        return pr_state
    return None


# --- Main Processing ---
def main():
    github_token = load_github_token(SETTINGS_FILE)
    if not github_token:
        print("GitHub token not found. Exiting.")
        return

    for file_path in INPUT_CSV_PATHS:
        processed_rows = []
        original_fieldnames = []
        author_col_already_exists = False

        print(f"\n--- Starting to process file '{file_path}' ---")

        # 1. Read the CSV file and get author information
        try:
            with open(file_path, mode='r', encoding='utf-8', newline='') as infile:
                reader = csv.DictReader(infile)
                original_fieldnames = reader.fieldnames

                # Check if the author_login column already exists
                if OUTPUT_AUTHOR_COL in original_fieldnames:
                    author_col_already_exists = True
                    print(f"  Column '{OUTPUT_AUTHOR_COL}' already exists. Skipping.")
                    continue  # Continue to the next file

                # Create new headers (add author_login to existing headers)
                new_fieldnames = original_fieldnames + [OUTPUT_AUTHOR_COL]

                print(f"  Fetching PR author information to add the '{OUTPUT_AUTHOR_COL}' column.")

                for row_num, row in enumerate(reader, 1):
                    owner = row.get(INPUT_OWNER_COL, "").strip()
                    repo_name = row.get(INPUT_REPO_COL, "").strip()
                    pr_number_str = row.get(INPUT_PR_NUM_COL, "").strip()

                    if not all([owner, repo_name, pr_number_str]):
                        print(
                            f"Warning: File '{file_path}', line {row_num}: Missing required info (owner, repo, pr_num). Skipping.")
                        row[OUTPUT_AUTHOR_COL] = "ERROR_MISSING_INFO"
                        processed_rows.append(row)
                        continue

                    try:
                        pr_number = int(pr_number_str)
                    except ValueError:
                        print(f"Warning: File '{file_path}', line {row_num}: Invalid PR number '{pr_number_str}'. Skipping.")
                        row[OUTPUT_AUTHOR_COL] = "ERROR_INVALID_PR_NUM"
                        processed_rows.append(row)
                        continue

                    print(f"  Fetching: {owner}/{repo_name} PR #{pr_number} (Row {row_num})...")
                    author_login = get_pr_author_from_api(owner, repo_name, pr_number, github_token)

                    row[OUTPUT_AUTHOR_COL] = author_login if author_login else "NOT_FOUND_API_ERROR"
                    processed_rows.append(row)

                    # Wait a bit to avoid API rate limiting
                    time.sleep(0.1)

        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found. Skipping.")
            continue
        except Exception as e:
            print(f"Error: An unexpected problem occurred while reading '{file_path}': {e}. Skipping.")
            continue

        # 2. Overwrite the original file with the processed data
        # To prevent data loss if processing is interrupted, only write back on success
        if processed_rows and not author_col_already_exists:
            print(f"  Overwriting '{file_path}' with processed data...")
            try:
                with open(file_path, mode='w', encoding='utf-8', newline='') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=new_fieldnames)
                    writer.writeheader()
                    writer.writerows(processed_rows)
                print(f"  Successfully updated '{file_path}'.")
            except Exception as e:
                print(f"Error: A problem occurred while overwriting '{file_path}': {e}")
        elif author_col_already_exists:
            print(f"  File '{file_path}' was already updated and was skipped.")
        else:
            print(f"  No processable data found for '{file_path}', so no changes were written.")


if __name__ == "__main__":
    main()