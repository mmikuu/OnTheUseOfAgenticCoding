import csv
import requests
import json
import os
import time

def load_github_token(settings_file):
    if not os.path.exists(settings_file):
        print(f"Error: Settings file '{settings_file}' not found.")
        print(f"Current working directory: {os.getcwd()}")
        return None
    try:
        with open(settings_file, 'r', encoding='utf-8') as f:
            token = f.readline().strip()
            if not token:
                print(f"Error: Settings file '{settings_file}' is empty or does not contain a valid token on the first line.")
                return None
            return token
    except Exception as e:
        print(f"Error: An issue occurred while reading the settings file '{settings_file}': {e}")
        return None

SETTINGS_FILENAME = "../setting.txt"
GITHUB_TOKEN = load_github_token(SETTINGS_FILENAME)

if GITHUB_TOKEN:
    HEADERS = {
        'Authorization': f'bearer {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'X-Github-Api-Version': '2022-11-28'
    }
    GRAPHQL_ENDPOINT = 'https://api.github.com/graphql'
else:
    HEADERS = {}
    print("Warning: GitHub token could not be loaded. The script will not function correctly.")

PR_FIRST_COMMIT_AND_BODY_QUERY = """
query PRFirstCommitAndBody($owner: String!, $repoName: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      number
      title
      bodyText
      commits(first: 1) {
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
            if is_rate_limited:
                print("Warning: GraphQL API rate limit reached. Skipping this PR for now.")
                return "RATE_LIMITED"
            else:
                print(f"  GraphQL API Error: {graphql_errors}")
                return None

        repository_data = data.get("data", {}).get("repository")
        if not repository_data:
            print(f"  Error: Repository data for {owner}/{repo_name} not found. Skipping.")
            return None

        pr_data_from_api = repository_data.get("pullRequest")
        if not pr_data_from_api:
            print(f"  Error: Data for PR #{pr_number} in {owner}/{repo_name} not found. Skipping.")
            return None

        pr_body_text = pr_data_from_api.get('bodyText', '')
        body_text_length = len(pr_body_text)

        first_commit_stats = {
            'changed_files': 0,
            'additions': 0,
            'deletions': 0
        }
        commits_nodes = pr_data_from_api.get("commits", {}).get("nodes", [])
        if commits_nodes:
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
            print(f"  This PR had no commits. Commit stats will be treated as 0.")

        return {
            'body_text_length': body_text_length,
            'first_commit_changed_files': first_commit_stats['changed_files'],
            'first_commit_additions': first_commit_stats['additions'],
            'first_commit_deletions': first_commit_stats['deletions'],
            'has_commits': bool(commits_nodes),
            'body_text': pr_body_text
        }

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"  HTTP Error 403 (Forbidden): Likely an API rate limit issue. Skipping this PR for now.")
            return "RATE_LIMITED"
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


if __name__ == '__main__':
    if not GITHUB_TOKEN:
        print("Error: GITHUB_TOKEN not loaded from settings.txt. Exiting script.")
    else:
        CSV_FILE_PATH = "../Data/RQ1-APR.csv"
        results_list = []
        try:
            with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                expected_headers = ['owner', 'repo_name', 'pull_number']
                if not reader.fieldnames or not all(col in reader.fieldnames for col in expected_headers):
                    print(f"Error: CSV file '{CSV_FILE_PATH}' is missing required headers: {expected_headers}.")
                else:
                    for row_number, row in enumerate(reader, 1):
                        repo_owner = row.get('owner')
                        repo_name = row.get('repo_name')
                        pr_number_str = row.get('pull_number')

                        if not all([repo_owner, repo_name, pr_number_str]):
                            print(f"Warning: Missing required info in CSV row {row_number}. Skipping.")
                            continue
                        try:
                            pr_number = int(pr_number_str)
                        except ValueError:
                            print(f"Warning: Invalid pr_number '{pr_number_str}' in CSV row {row_number}. Skipping.")
                            continue

                        pr_data = get_pr_first_commit_and_body_data(repo_owner, repo_name, pr_number, GITHUB_TOKEN)

                        if pr_data == "RATE_LIMITED":
                            print(
                                f"Skipped PR #{pr_number} for {repo_owner}/{repo_name} due to rate limit. Waiting 60 seconds...")
                            time.sleep(60)
                            print("-" * 30)
                            continue

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
                            print(f"  Result: Body text length={pr_data['body_text_length']}")
                        else:
                            print(f"  Failed to retrieve or process data for PR #{pr_number} in {repo_owner}/{repo_name}.")
                        print("-" * 30)
                        time.sleep(1.5)
            if results_list:
                print("\n\n--- All PRs Processed Summary (First Commit Only) ---")
                output_csv_filename = "../Data/RQ1-APR-first-revision.csv"
                try:
                    fieldnames = ['owner', 'repo_name', 'pr_number', 'body_text_length',
                                  'first_commit_changed_files', 'first_commit_additions', 'first_commit_deletions',
                                  'has_commits', 'body_text']
                    with open(output_csv_filename, mode='w', newline='', encoding='utf-8') as outfile:
                        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(results_list)
                    print(f"\nSummary results saved to '{output_csv_filename}'.")
                except IOError:
                    print(f"Error: Could not write results to CSV file '{output_csv_filename}'.")
            else:
                print("\nNo PRs were processed, or data retrieval failed for all PRs.")

        except FileNotFoundError:
            print(f"Error: Input CSV file '{CSV_FILE_PATH}' not found.")
        except Exception as e:
            print(f"An unexpected error occurred in the main process: {e}")
