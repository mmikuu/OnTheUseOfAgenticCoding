import csv
import requests
import json
import time

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

# GraphQL query that supports pagination
PR_COMMITS_QUERY = """
query PRAllCommitsStats($owner: String!, $repoName: String!, $prNumber: Int!, $afterCursor: String) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      number
      title
      # Final change volume for the entire PR (for comparison)
      # changedFiles
      # additions
      # deletions
      commits(first: 100, after: $afterCursor) { # Get up to 100 commits per page
        pageInfo {
          endCursor
          hasNextPage
        }
        totalCount # Total number of commits in the PR
        nodes {
          commit {
            oid
            messageHeadline
            # Changes in this commit alone (diff from parent commit)
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


def get_all_pr_commits_data(owner, repo_name, pr_number, token):
    """
    Gets all commit data for a specified PR using the GitHub GraphQL API (with pagination support).
    """
    all_commits_nodes = []
    has_next_page = True
    after_cursor = None
    headers = {
        "Authorization": f"bearer {token}",
        "Content-Type": "application/json"
    }

    print(f"Processing: {owner}/{repo_name} PR #{pr_number}")
    page_num = 1
    total_commits_count_pr = 0

    while has_next_page:
        variables = {
            "owner": owner,
            "repoName": repo_name,
            "prNumber": pr_number,
            "afterCursor": after_cursor
        }
        payload = {
            "query": PR_COMMITS_QUERY,
            "variables": variables
        }

        try:
            response = requests.post(GRAPHQL_ENDPOINT, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                graphql_errors = data['errors']
                # Check for rateLimitExceeded error
                is_rate_limited = any(err.get('type') == 'RATE_LIMITED' for err in graphql_errors)
                if is_rate_limited:
                    print("Warning: GraphQL API rate limit reached. Waiting for 60 seconds...")
                    time.sleep(60)  # Wait for 60 seconds
                    continue  # Retrying on the same page
                else:
                    print(f"  GraphQL API Error: {graphql_errors}")
                    return None  # For other errors, skip processing this PR

            pr_data = data.get("data", {}).get("repository", {}).get("pullRequest")
            if not pr_data:
                # e.g., if the PR does not exist or access is denied
                print(f"  Error: Data for {owner}/{repo_name} PR #{pr_number} not found. Skipping.")
                return None

            commits_connection = pr_data.get("commits", {})
            if page_num == 1:
                total_commits_count_pr = commits_connection.get('totalCount', 0)
                print(f"  Total number of commits in PR: {total_commits_count_pr}")
                if total_commits_count_pr == 0:  # End processing for PRs with no commits
                    print(f"  This PR has no commits. Skipping.")
                    return []

            nodes = commits_connection.get("nodes", [])
            all_commits_nodes.extend(nodes)

            page_info = commits_connection.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

            page_num += 1
            time.sleep(1)  # Optional short wait to reduce API load

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:  # Forbidden, often due to rate limits or auth issues
                print(f"  HTTP Error 403 (Forbidden): Likely an API rate limit issue. Waiting for 60 seconds...")
                time.sleep(60)
                continue  # Retrying on the same page
            else:
                print(f"  HTTP Request Error: {e} (Status code: {e.response.status_code})")
                return None  # Skip processing for this PR
        except requests.exceptions.RequestException as e:
            print(f"  HTTP Request Error: {e}")
            return None
        except ValueError as e:
            print(f"  JSON Decode Error: {e}")
            print(f"  Response content: {response.text if 'response' in locals() else 'N/A'}")
            return None
        except Exception as e:
            print(f"  An unexpected error occurred: {e}")
            return None

    if len(all_commits_nodes) == total_commits_count_pr:
        print(f"  Successfully fetched all {total_commits_count_pr} commits.")
    else:
        print(
            f"  Warning: The total number of commits in the PR ({total_commits_count_pr}) does not match the number of fetched nodes ({len(all_commits_nodes)}).")

    return all_commits_nodes


def sum_commit_stats(all_commits_data):
    """
    Sums the statistics (changed files, additions, deletions) for all commits in a PR.
    """
    total_changed_files_sum = 0
    total_additions_sum = 0
    total_deletions_sum = 0

    if not isinstance(all_commits_data, list):
        return {
            'total_changed_files': 0,
            'total_additions': 0,
            'total_deletions': 0
        }

    for commit_node in all_commits_data:
        commit_stats = commit_node.get('commit') if isinstance(commit_node, dict) else None
        if isinstance(commit_stats, dict):
            changed_files = commit_stats.get('changedFilesIfAvailable')
            total_changed_files_sum += changed_files if isinstance(changed_files, (int, float)) else 0

            additions = commit_stats.get('additions')
            total_additions_sum += additions if isinstance(additions, (int, float)) else 0

            deletions = commit_stats.get('deletions')
            total_deletions_sum += deletions if isinstance(deletions, (int, float)) else 0

    return {
        'total_changed_files': total_changed_files_sum,
        'total_additions': total_additions_sum,
        'total_deletions': total_deletions_sum
    }


# --- Main Processing ---
if __name__ == '__main__':
    if not GITHUB_TOKEN:
        print("Error: GITHUB_TOKEN is not configured. Please check your settings file. Exiting.")
    else:
        results = []  # A list to store the results for each PR
        try:
            with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if not reader.fieldnames or not all(col in reader.fieldnames for col in ['owner', 'repo_name', 'pull_number']):
                    print(
                        f"Error: CSV file '{CSV_FILE_PATH}' is missing required header columns: 'owner', 'repo_name', 'pull_number'.")
                else:
                    for row_number, row in enumerate(reader, 1):
                        repo_owner = row.get('owner')
                        repo_name = row.get('repo_name')
                        pr_number_str = row.get('pull_number')

                        if not all([repo_owner, repo_name, pr_number_str]):
                            print(
                                f"Warning: Missing required information (owner, repo, pr_number) in CSV row {row_number}. Skipping.")
                            continue

                        try:
                            pr_number = int(pr_number_str)
                        except ValueError:
                            print(
                                f"Warning: PR number '{pr_number_str}' in CSV row {row_number} is not a number. Skipping.")
                            continue

                        # Get all commit data for the specified PR
                        all_commits = get_all_pr_commits_data(repo_owner, repo_name, pr_number, GITHUB_TOKEN)
                        print(all_commits)
                        if all_commits is not None:  # Aggregate only if data fetching was successful
                            if len(all_commits) > 1:
                                commits_to_sum = all_commits[1:]  # List excluding the first commit
                                num_commits_actually_summed = len(commits_to_sum)
                                print(f"  Aggregating {num_commits_actually_summed} commits (excluding the first).")
                            elif len(all_commits) == 1:
                                commits_to_sum = []  # If there's only one commit, this becomes empty
                                num_commits_actually_summed = 0
                                print(f"  Only one commit exists; no revisions to aggregate after excluding the first.")
                            else:  # If all_commits is an empty list
                                commits_to_sum = []
                                num_commits_actually_summed = 0
                                print(f"  No commits to aggregate.")
                            totals = sum_commit_stats(commits_to_sum)
                            pr_result = {
                                'owner': repo_owner,
                                'repo': repo_name,
                                'pr_number': pr_number,
                                'sum_changed_files': totals['total_changed_files'],
                                'sum_additions': totals['total_additions'],
                                'sum_deletions': totals['total_deletions'],
                                'commit': len(all_commits)
                            }
                            results.append(pr_result)
                            print(
                                f"  Result: Files={totals['total_changed_files']}, Additions={totals['total_additions']}, Deletions={totals['total_deletions']}")
                        else:
                            print(f"  Failed to fetch commit data for {repo_owner}/{repo_name} PR #{pr_number}.")
                        print("-" * 30)  # Separator for each PR's processing
                        time.sleep(2)  # Wait before processing the next PR (to respect API rate limits)

            # --- Display all PR results together (or output to a file) ---
            if results:
                print("\n\n--- Aggregated Results for All PRs ---")
                for res in results:
                    print(f"Repo: {res['owner']}/{res['repo']}, PR: #{res['pr_number']}, "
                          f"Fetched Commits: {res['commit']}, "
                          f"Sum Changed Files: {res['sum_changed_files']}, "
                          f"Sum Additions: {res['sum_additions']}, "
                          f"Sum Deletions: {res['sum_deletions']}")

                # Example of outputting results to a CSV file
                output_csv_filename = "../Data/RQ2-HPR-revision_second_to_last.csv"
                try:
                    with open(output_csv_filename, mode='w', newline='', encoding='utf-8') as outfile:
                        fieldnames = ['owner', 'repo', 'pr_number', 'commit', 'sum_changed_files',
                                      'sum_additions', 'sum_deletions']
                        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(results)
                    print(f"\nAggregated results saved to '{output_csv_filename}'.")
                except IOError:
                    print(f"Error: Could not write results to CSV file '{output_csv_filename}'.")
            else:
                print("\nNo PRs were processed, or data fetching failed for all PRs.")


        except FileNotFoundError:
            print(f"Error: CSV file '{CSV_FILE_PATH}' not found.")
        except Exception as e:
            print(f"An unexpected error occurred in the main process: {e}")