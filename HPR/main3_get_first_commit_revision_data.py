import csv
import requests
import json
import time  # For minimal waits to respect API rate limits

# --- Configuration (Items to be set by the user) ---
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

SETTINGS_FILE = "../setting.txt" # Path to the file containing the PAT
GITHUB_TOKEN = load_token(SETTINGS_FILE)
CSV_FILE_PATH = "../Data/RQ1-HPR.csv"  # Path to the CSV file containing PR information
# -------------------------------------------------

GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

# GraphQL query to get the PR's bodyText and first commit information
PR_FIRST_COMMIT_AND_BODY_QUERY = """
query PRFirstCommitAndBody($owner: String!, $repoName: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      number
      title
      bodyText # PR's Description (plain text)
      commits(first: 1) { # Get only the first commit
        nodes {
          commit {
            oid
            messageHeadline
            changedFilesIfAvailable
            additions
            deletions
          }
        }
      }
    }
  }
}
"""


def get_pr_first_commit_and_body_data(owner, repo_name, pr_number, token):
    """
    Gets the bodyText and first commit statistics for a specified PR using the GitHub GraphQL API.
    Returns:
        dict: A dictionary containing PR metadata and first commit statistics. Returns None on error.
    """
    headers = {
        "Authorization": f"bearer {token}",
        "Content-Type": "application/json"
    }
    variables = {
        "owner": owner,
        "repoName": repo_name,
        "prNumber": pr_number
    }
    payload = {
        "query": PR_FIRST_COMMIT_AND_BODY_QUERY,
        "variables": variables
    }

    print(f"Processing: {owner}/{repo_name} PR #{pr_number}")

    try:
        response = requests.post(GRAPHQL_ENDPOINT, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            graphql_errors = data['errors']
            is_rate_limited = any(err.get('type') == 'RATE_LIMITED' for err in graphql_errors)
            if is_rate_limited:  # If rate limited, return None for this PR (main loop will handle retry logic)
                print("Warning: GraphQL API rate limit reached. This PR will be temporarily skipped.")
                return "RATE_LIMITED"  # Return a special value to indicate rate limiting
            else:
                print(f"  GraphQL API Error: {graphql_errors}")
                return None

        repository_data = data.get("data", {}).get("repository")
        if not repository_data:
            print(f"  Error: Repository data for {owner}/{repo_name} not found. Skipping.")
            return None

        pr_data_from_api = repository_data.get("pullRequest")
        if not pr_data_from_api:
            print(f"  Error: Data for {owner}/{repo_name} PR #{pr_number} not found. Skipping.")
            return None

        # PR metadata
        pr_body_text = pr_data_from_api.get('bodyText', '')
        body_text_length = len(pr_body_text)


        # First commit statistics
        first_commit_stats = {
            'changed_files': 0,
            'additions': 0,
            'deletions': 0
        }
        commits_nodes = pr_data_from_api.get("commits", {}).get("nodes", [])
        if commits_nodes:  # If commits exist
            first_commit_data = commits_nodes[0].get('commit', {})
            if first_commit_data:
                changed_files = first_commit_data.get('changedFilesIfAvailable')
                first_commit_stats['changed_files'] = changed_files if isinstance(changed_files, (int, float)) else 0

                additions = first_commit_data.get('additions')
                first_commit_stats['additions'] = additions if isinstance(additions, (int, float)) else 0

                deletions = first_commit_data.get('deletions')
                first_commit_stats['deletions'] = deletions if isinstance(deletions, (int, float)) else 0
                print(
                    f"  First commit info: Files={first_commit_stats['changed_files']}, Additions={first_commit_stats['additions']}, Deletions={first_commit_stats['deletions']}")
        else:
            print(f"  This PR had no commits. Commit statistics will be treated as 0.")

        return {
            'body_text_length': body_text_length,
            'first_commit_changed_files': first_commit_stats['changed_files'],
            'first_commit_additions': first_commit_stats['additions'],
            'first_commit_deletions': first_commit_stats['deletions'],
            'has_commits': bool(commits_nodes),# Flag for whether a commit existed
            'body_text': pr_body_text
        }

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"  HTTP Error 403 (Forbidden): Likely due to API rate limit. This PR will be temporarily skipped.")
            return "RATE_LIMITED"  # Indicate rate limiting
        else:
            print(f"  HTTP Request Error: {e} (Status code: {e.response.status_code})")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  HTTP Request Error: {e}")
        return None
    except ValueError as e:  # Includes JSONDecodeError
        print(f"  JSON Decode Error: {e}")
        print(f"  Response content: {response.text if 'response' in locals() else 'N/A'}")
        return None
    except Exception as e:
        print(f"  An unexpected error occurred: {e}")
        return None


# --- Main Processing ---
if __name__ == '__main__':
    if not GITHUB_TOKEN:
        print("Error: GitHub token not loaded. Please check your settings file. Exiting.")
        exit()

    results_list = []  # A list to store the results for each PR
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            expected_headers = ['owner', 'repo_name', 'pull_number']  # Required headers in the CSV
            if not reader.fieldnames or not all(col in reader.fieldnames for col in expected_headers):
                print(f"Error: CSV file '{CSV_FILE_PATH}' is missing required header columns: {expected_headers}.")
            else:
                for row_number, row in enumerate(reader, 1):
                    repo_owner = row.get('owner')
                    repo_name = row.get('repo_name')
                    pr_number_str = row.get('pull_number')

                    if not all([repo_owner, repo_name, pr_number_str]):
                        print(f"Warning: Missing information in CSV row {row_number}. Skipping.")
                        continue
                    try:
                        pr_number = int(pr_number_str)
                    except ValueError:
                        print(f"Warning: PR number '{pr_number_str}' in CSV row {row_number} is not a number. Skipping.")
                        continue

                    pr_data = get_pr_first_commit_and_body_data(repo_owner, repo_name, pr_number, GITHUB_TOKEN)

                    if pr_data == "RATE_LIMITED":
                        print(
                            f"Skipping {repo_owner}/{repo_name} PR #{pr_number} due to rate limit. Waiting 60 seconds...")
                        time.sleep(60)  # Wait for a longer period if rate limited
                        # Logic to retry this PR later could be added here if needed
                        print("-" * 30)
                        continue  # Continue to the next PR

                    if pr_data is not None:
                        result_row = {
                            'owner': repo_owner,
                            'repo_name': repo_name,
                            'pr_number': pr_number,
                            'body_text_length': pr_data['body_text_length'],
                            'first_commit_changed_files': pr_data['first_commit_changed_files'],
                            'first_commit_additions': pr_data['first_commit_additions'],
                            'first_commit_deletions': pr_data['first_commit_deletions'],
                            'has_commits': pr_data['has_commits'],
                            'body_text': pr_data['body_text']
                        }
                        results_list.append(result_row)
                        print(f"  Result: BodyText length={pr_data['body_text_length']}")
                    else:
                        print(f"  Failed to fetch or process data for {repo_owner}/{repo_name} PR #{pr_number}.")
                    print("-" * 30)
                    time.sleep(1.5)  # Short wait between the next API call
        if results_list :
            print("\n\n--- Aggregated Results for All PRs (First Commit Only) ---")
            output_csv_filename = "../Data/RQ1-HPR-first-revision.csv"  # Changed output filename
            try:
                fieldnames = ['owner', 'repo_name', 'pr_number', 'body_text_length',
                              'first_commit_changed_files', 'first_commit_additions', 'first_commit_deletions',
                              'has_commits','body_text']
                with open(output_csv_filename, mode='w', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results_list)
                print(f"\nAggregated results saved to '{output_csv_filename}'.")
            except IOError:
                print(f"Error: Could not write results to CSV file '{output_csv_filename}'.")
        else:
            print("\nNo PRs were processed, or data fetching failed for all PRs.")

    except FileNotFoundError:
        print(f"Error: CSV file '{CSV_FILE_PATH}' not found.")
    except Exception as e:
        print(f"An unexpected error occurred in the main process: {e}")