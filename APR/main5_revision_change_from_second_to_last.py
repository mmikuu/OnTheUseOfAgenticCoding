import csv
import requests
import json
import os
import time


def load_github_token(settings_file="../setting.txt"):
    if not os.path.exists(settings_file):
        print(f"Error: Settings file '{settings_file}' not found.")
        print(f"Current working directory: {os.getcwd()}")
        return None
    try:
        with open(settings_file, 'r', encoding='utf-8') as f:
            token = f.readline().strip()
            if not token:
                print(
                    f"Error: Settings file '{settings_file}' is empty or does not contain a valid token on the first line.")
                return None
            return token
    except Exception as e:
        print(f"Error: An issue occurred while reading the settings file '{settings_file}': {e}")
        return None


GITHUB_TOKEN = load_github_token()

if GITHUB_TOKEN:
    HEADERS = {
        'Authorization': f'bearer {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'X-Github-Api-Version': '2022-11-28'
    }
    GRAPHQL_URL = 'https://api.github.com/graphql'
    REST_API_BASE_URL = "https://api.github.com"
else:
    HEADERS = {}
    print("Warning: GitHub token could not be loaded. The script will not function correctly.")

PR_COMMITS_QUERY = """
query PRAllCommitsStats($owner: String!, $repoName: String!, $prNumber: Int!, $afterCursor: String) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      number
      title
      commits(first: 100, after: $afterCursor) {
        pageInfo {
          endCursor
          hasNextPage
        }
        totalCount
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


def get_all_pr_commits_data(owner, repo_name, pr_number, token):
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
                is_rate_limited = any(err.get('type') == 'RATE_LIMITED' for err in graphql_errors)
                if is_rate_limited:
                    print("Warning: GraphQL API rate limit reached. Waiting for 60 seconds...")
                    time.sleep(60)
                    continue
                else:
                    print(f"  GraphQL API Error: {graphql_errors}")
                    return None

            pr_data = data.get("data", {}).get("repository", {}).get("pullRequest")
            if not pr_data:
                print(f"  Error: Data for {owner}/{repo_name} PR #{pr_number} not found. Skipping.")
                return None

            commits_connection = pr_data.get("commits", {})
            if page_num == 1:
                total_commits_count_pr = commits_connection.get('totalCount', 0)
                print(f"  Total commits in PR: {total_commits_count_pr}")
                if total_commits_count_pr == 0:
                    print(f"  This PR has no commits. Skipping.")
                    return []

            nodes = commits_connection.get("nodes", [])
            all_commits_nodes.extend(nodes)

            page_info = commits_connection.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

            page_num += 1
            time.sleep(1)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print(f"  HTTP Error 403 (Forbidden): Possible API rate limit. Waiting 60 seconds...")
                time.sleep(60)
                continue
            else:
                print(f"  HTTP Request Error: {e} (Status code: {e.response.status_code})")
                return None
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
        print(f"  Successfully retrieved all {total_commits_count_pr} commits.")
    else:
        print(
            f"  Warning: Total commits in PR ({total_commits_count_pr}) does not match the number of nodes retrieved ({len(all_commits_nodes)}).")

    return all_commits_nodes


def sum_commit_stats(all_commits_data):
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


if __name__ == '__main__':
    if not GITHUB_TOKEN:
        print("Error: GITHUB_TOKEN not loaded from settings.txt. Exiting script.")
    else:
        CSV_FILE_PATH = "../Data/RQ1-APR.csv"
        results = []
        try:
            with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if not reader.fieldnames or not all(
                        col in reader.fieldnames for col in ['owner', 'repo_name', 'pull_number']):
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
                                f"Warning: Invalid pr_number '{pr_number_str}' in CSV row {row_number}. Skipping.")
                            continue

                        all_commits = get_all_pr_commits_data(repo_owner, repo_name, pr_number, GITHUB_TOKEN)

                        if all_commits is not None:
                            if len(all_commits) > 1:
                                commits_to_sum = all_commits[1:]
                                num_commits_actually_summed = len(commits_to_sum)
                                print(f"  Aggregating {num_commits_actually_summed} commits (excluding the first).")
                            elif len(all_commits) == 1:
                                commits_to_sum = []
                                num_commits_actually_summed = 0
                                print(f"  Only one commit found; nothing to aggregate after excluding the first.")
                            else:
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
                            print(f"  Failed to retrieve commit data for {repo_owner}/{repo_name} PR #{pr_number}.")
                        print("-" * 30)
                        time.sleep(2)

            if results:
                print("\n\n--- All PRs Aggregated Results ---")
                for res in results:
                    print(f"Repo: {res['owner']}/{res['repo']}, PR: #{res['pr_number']}, "
                          f"Fetched Commits: {res['commit']}, "
                          f"Sum Changed Files: {res['sum_changed_files']}, "
                          f"Sum Additions: {res['sum_additions']}, "
                          f"Sum Deletions: {res['sum_deletions']}")

                output_csv_filename = "../Data/RQ2-APR-changefile-Name.csv"
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
                print("\nNo PRs were processed, or data retrieval failed for all PRs.")

        except FileNotFoundError:
            print(f"Error: Input CSV file '{CSV_FILE_PATH}' not found.")
        except Exception as e:
            print(f"An unexpected error occurred in the main process: {e}")
