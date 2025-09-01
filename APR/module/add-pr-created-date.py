import csv
import requests
import time
import os

# --- Configuration ---
# It is strongly recommended to get the GitHub Personal Access Token (PAT) from an environment variable or a file.
# Set it like: export GITHUB_TOKEN="your_pat_here"
SETTINGS_FILE = "../../setting.txt"

GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

# List of CSV files to process
INPUT_CSV_PATHS = [
    "../../data/commit_details/APR-commit-details.csv",
    "../../data/commit_details/HPR-commit-details.csv"
]

# Input CSV column names
INPUT_OWNER_COL = "owner"
INPUT_REPO_COL = "repo_name"
INPUT_PR_NUM_COL = "pull_number"
# --- Change 1 ---
# Changed the output column name from "author_login" to "pr_created_date"
OUTPUT_CREATED_AT_COL = "pr_created_date"

# --- Change 2 ---
# Changed the GraphQL query to get createdAt (creation timestamp) instead of author
PR_CREATED_AT_QUERY = """
query GetPrCreatedAt($owner: String!, $repoName: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      createdAt
    }
  }
}
"""


# --- Helper Functions ---

def load_token(file_path):
    """Loads the GitHub API token from the first line of a settings file."""
    try:
        with open(file_path, 'r') as f:
            token = f.readline().strip()
            if not token:
                print(f"Error: The first line of '{file_path}' is empty. Please write the token in the file.")
                return None
            return token
    except FileNotFoundError:
        print(f"Error: Token file '{file_path}' not found.")
        print("Please create the file and write your GitHub PAT inside.")
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
        response.raise_for_status()
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

# --- Change 3 ---
# Changed the function to get the PR's creation timestamp
def get_pr_created_at_from_api(owner, repo_name, pr_number, token):
    """Gets the creation timestamp for a specific PR."""
    variables = {
        "owner": owner,
        "repoName": repo_name,
        "prNumber": int(pr_number)
    }
    # Use the modified query
    data = run_graphql_query(PR_CREATED_AT_QUERY, variables, token)
    if data:
        # Extract createdAt from the JSON response
        created_at = data.get("data", {}).get("repository", {}).get("pullRequest", {}).get("createdAt")
        return created_at
    return None


# --- Main Processing ---
def main():
    github_token = load_token(SETTINGS_FILE)
    if not github_token:
        return

    for file_path in INPUT_CSV_PATHS:
        processed_rows = []
        original_fieldnames = []
        created_at_col_already_exists = False

        print(f"\n--- Starting to process file '{file_path}' ---")

        # 1. Read the CSV file and get creation timestamp information
        try:
            with open(file_path, mode='r', encoding='utf-8', newline='') as infile:
                reader = csv.DictReader(infile)
                original_fieldnames = reader.fieldnames

                # --- Change 4 ---
                # Check if the pr_created_date column already exists
                if OUTPUT_CREATED_AT_COL in original_fieldnames:
                    created_at_col_already_exists = True
                    print(f"  Column '{OUTPUT_CREATED_AT_COL}' already exists. Skipping.")
                    for row in reader:
                        processed_rows.append(row)
                    continue

                # Create new headers
                new_fieldnames = original_fieldnames + [OUTPUT_CREATED_AT_COL]

                print(f"  Fetching PR creation date information to add the '{OUTPUT_CREATED_AT_COL}' column.")

                for row_num, row in enumerate(reader, 1):
                    owner = row.get(INPUT_OWNER_COL, "").strip()
                    repo_name = row.get(INPUT_REPO_COL, "").strip()
                    pr_number_str = row.get(INPUT_PR_NUM_COL, "").strip()

                    if not all([owner, repo_name, pr_number_str]):
                        print(f"Warning: File '{file_path}', line {row_num}: Missing required information. Skipping.")
                        row[OUTPUT_CREATED_AT_COL] = "ERROR_MISSING_INFO"
                        processed_rows.append(row)
                        continue

                    try:
                        pr_number = int(pr_number_str)
                    except ValueError:
                        print(f"Warning: File '{file_path}', line {row_num}: Invalid PR number '{pr_number_str}'. Skipping.")
                        row[OUTPUT_CREATED_AT_COL] = "ERROR_INVALID_PR_NUM"
                        processed_rows.append(row)
                        continue

                    print(f"  Fetching: {owner}/{repo_name} PR #{pr_number} (Row {row_num})...")
                    # Call the modified function
                    pr_created_at = get_pr_created_at_from_api(owner, repo_name, pr_number, github_token)

                    # Add the fetched creation timestamp to the row
                    row[OUTPUT_CREATED_AT_COL] = pr_created_at if pr_created_at else "NOT_FOUND_API_ERROR"
                    processed_rows.append(row)

                    time.sleep(0.1)

        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found. Skipping.")
            continue
        except Exception as e:
            print(f"Error: An unexpected problem occurred while reading file '{file_path}': {e}. Skipping.")
            continue

        # 2. Overwrite the original file with the processed data
        if processed_rows and not created_at_col_already_exists:
            print(f"  Overwriting '{file_path}' with processed data...")
            try:
                with open(file_path, mode='w', encoding='utf-8', newline='') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=new_fieldnames)
                    writer.writeheader()
                    writer.writerows(processed_rows)
                print(f"  Successfully updated '{file_path}'.")
            except Exception as e:
                print(f"Error: A problem occurred while overwriting file '{file_path}': {e}")
        elif created_at_col_already_exists:
            print(f"  File '{file_path}' was already updated and was skipped.")
        else:
            print(f"  No processable data was found in file '{file_path}', so no changes were written.")


if __name__ == "__main__":
    main()