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
                print(f"Error: Settings file '{settings_file}' is empty or does not contain a valid token on the first line.")
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
query GetPrCommits($owner: String!, $repoName: String!, $prNumber: Int!, $afterCursor: String) {
  repository(owner: $owner, name: $repoName) {
    pullRequest(number: $prNumber) {
      commits(first: 100, after: $afterCursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          commit {
            oid
            authoredDate
          }
        }
      }
    }
  }
}
"""


def run_graphql_query(query, variables):
    for attempt in range(3):
        try:
            graphql_headers = HEADERS.copy()
            graphql_headers['Content-Type'] = 'application/json'
            response = requests.post(GRAPHQL_URL, headers=graphql_headers,
                                     json={'query': query, 'variables': variables},
                                     timeout=60)
            response.raise_for_status()
            result = response.json()
            if 'errors' in result and result['errors']:
                if any("RATE_LIMITED" in err.get("type", "") for err in result["errors"]):
                    wait_time = 60 * (attempt + 1)
                    print(f"Warning: GraphQL rate limit. Waiting for {wait_time} seconds...")
                    time.sleep(wait_time)
                    if attempt < 2:
                        continue
                    else:
                        raise Exception(f"GraphQL RATE_LIMITED retries failed: {result['errors']}")

                if any("NOT_FOUND" in err.get("type", "") for err in result["errors"]):
                    print(f"Warning: GraphQL entity not found. Errors: {result['errors']}")
                    return {"data": None, "errors": result['errors']}

                raise Exception(f"GraphQL query failed: {result['errors']}")
            return result
        except requests.exceptions.ReadTimeout:
            print(f"Warning: GraphQL request timed out. Waiting for {10 * (attempt + 1)} seconds...")
            if attempt == 2: raise
            time.sleep(10 * (attempt + 1))
        except requests.exceptions.RequestException as e_req:
            print(f"Warning: GraphQL request error: {e_req}. Waiting for {10 * (attempt + 1)} seconds...")
            if attempt == 2: raise
            time.sleep(10 * (attempt + 1))

    raise Exception("GraphQL query execution failed after multiple retries.")


def get_files_changed_in_single_commit_rest(owner, repo_name, commit_sha):
    url = f"{REST_API_BASE_URL}/repos/{owner}/{repo_name}/commits/{commit_sha}"
    changed_files = []

    for attempt in range(3):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)

            if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers and int(
                    response.headers['X-RateLimit-Remaining']) == 0:
                reset_time_str = response.headers.get('X-RateLimit-Reset', str(int(time.time() + 60)))
                reset_time = int(reset_time_str)
                wait_duration = max(1, reset_time - int(time.time())) + 5
                print(f"Warning: REST API rate limit. Waiting for {wait_duration} seconds...")
                time.sleep(wait_duration)
                if attempt < 2:
                    continue
                else:
                    print("REST API rate limit: retries exceeded.")
                    return []

            response.raise_for_status()
            commit_data = response.json()

            if 'files' in commit_data and isinstance(commit_data['files'], list):
                for file_info in commit_data['files']:
                    if isinstance(file_info, dict) and 'filename' in file_info:
                        changed_files.append(file_info['filename'])
            else:
                print(f"Warning: 'files' array not found in REST API response for commit {commit_sha}.")

            return changed_files

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Warning: Commit {commit_sha} (REST) not found (404 error). URL: {url}")
                return []
            elif e.response.status_code == 422:
                print(f"Warning: Unprocessable commit {commit_sha} (REST) (422 error). It might be too large. URL: {url}")
                return []
            else:
                print(
                    f"REST API HTTP error (commit {commit_sha}, attempt {attempt + 1}): {e.response.status_code} {e.response.reason}")
                if attempt == 2:
                    print(f"  HTTP error: Retries exceeded. Response: {e.response.text if e.response else 'N/A'}")
                    return []
        except requests.exceptions.RequestException as e_req:
            print(f"REST API request error (commit {commit_sha}, attempt {attempt + 1}): {e_req}")
            if attempt == 2:
                print(f"  Request error: Retries exceeded.")
                return []
        except json.JSONDecodeError as e_json:
            print(f"JSON decode error for REST API response (commit {commit_sha}): {e_json}")
            return []

        if attempt < 2:
            time.sleep(5 * (attempt + 1))

    return []


def get_pr_commit_shas_sorted(owner, repo_name, pr_number):
    all_commits_info = []
    commits_after_cursor = None
    while True:
        variables = {
            "owner": owner,
            "repoName": repo_name,
            "prNumber": pr_number,
            "afterCursor": commits_after_cursor
        }
        result = run_graphql_query(PR_COMMITS_QUERY, variables)

        if result.get("data") is None and result.get("errors"):
            print(
                f"Warning: GraphQL error while fetching commit info for PR #{pr_number} ({owner}/{repo_name}). Errors: {result.get('errors')}")
            return []

        pr_data = result.get('data', {}).get('repository', {}).get('pullRequest')
        if not pr_data or 'commits' not in pr_data:
            if pr_data is None and result.get("data", {}).get("repository") is not None:
                print(f"Info: PR #{pr_number} ({owner}/{repo_name}) not found via GraphQL.")
            else:
                print(f"Warning: Could not retrieve GraphQL commit information for PR #{pr_number} ({owner}/{repo_name}).")
            break

        commits_data = pr_data['commits']
        for node in commits_data.get('nodes', []):
            commit_info = node.get('commit')
            if isinstance(commit_info, dict) and 'oid' in commit_info and 'authoredDate' in commit_info:
                all_commits_info.append({
                    "oid": commit_info['oid'],
                    "authoredDate": commit_info['authoredDate']
                })
            else:
                print(f"Warning: Invalid GraphQL commit node format for PR #{pr_number}: {node}")

        page_info = commits_data.get('pageInfo', {})
        if page_info.get('hasNextPage'):
            commits_after_cursor = page_info.get('endCursor')
        else:
            break

    all_commits_info.sort(key=lambda x: x['authoredDate'])
    return [commit['oid'] for commit in all_commits_info]


def analyze_pr_from_csv_row(row_data, csv_writer):
    owner = row_data.get('owner')
    repo_name = row_data.get('repo_name')
    pull_number_str = row_data.get('pull_number')
    url = row_data.get('url', '')

    if not all([owner, repo_name, pull_number_str]):
        print(f"Error: Missing required PR info (owner, repo_name, pull_number). Skipping: {row_data}")
        return

    try:
        pr_number = int(pull_number_str)
    except ValueError:
        print(f"Error: Invalid PR number '{pull_number_str}' ({owner}/{repo_name}). Skipping.")
        return

    print(f"\nAnalyzing: {owner}/{repo_name}#{pr_number}")

    print("  Fetching all commit SHAs for PR via GraphQL...")
    pr_all_commit_shas = get_pr_commit_shas_sorted(owner, repo_name, pr_number)

    if not pr_all_commit_shas:
        print(f"  Error: Failed to retrieve commit list for {owner}/{repo_name}#{pr_number}. Skipping.")
        csv_writer.writerow([owner, repo_name, pr_number, url, "ERROR: No commits found", "ERROR: No commits found"])
        return

    api_first_commit_sha = pr_all_commit_shas[0]
    print(f"  Fetching changed files for the first commit ({api_first_commit_sha}) via REST API...")
    first_commit_change_files_list = get_files_changed_in_single_commit_rest(owner, repo_name, api_first_commit_sha)
    first_commit_changefile_str = ";".join(
        sorted(first_commit_change_files_list)) if first_commit_change_files_list else ""

    all_changed_files_in_pr_set = set(first_commit_change_files_list)

    if len(pr_all_commit_shas) > 1:
        commits_for_subsequent_analysis = pr_all_commit_shas[1:]
        print(f"  Fetching changed files for subsequent commits ({len(commits_for_subsequent_analysis)})...")
        for i, commit_sha_in_range in enumerate(commits_for_subsequent_analysis):
            files = get_files_changed_in_single_commit_rest(owner, repo_name, commit_sha_in_range)
            if files:
                all_changed_files_in_pr_set.update(files)

    total_changefile_str = ";".join(sorted(list(all_changed_files_in_pr_set))) if all_changed_files_in_pr_set else ""

    csv_writer.writerow([owner, repo_name, pr_number, url, first_commit_changefile_str, total_changefile_str])
    print(f"  Analysis complete and CSV row written: {owner}/{repo_name}#{pr_number}")


def main():
    input_csv_file = '../Data/RQ1-APR.csv'
    output_csv_file = '../Data/RQ2-APR-changefile-Name.csv'

    if not GITHUB_TOKEN:
        print("Error: Could not load GITHUB_TOKEN from setting.txt. Exiting script.")
        return

    csv_header = ['owner', 'repo_name', 'pull_number', 'url', 'first_commit_changefile', 'total_changefile']

    try:
        with open(input_csv_file, mode='r', encoding='utf-8-sig', newline='') as infile, \
                open(output_csv_file, mode='w', encoding='utf-8', newline='') as outfile:

            reader = csv.DictReader(infile)
            writer = csv.writer(outfile)

            if not reader.fieldnames:
                print(f"Error: Input CSV file '{input_csv_file}' is empty or has no header.")
                return

            required_input_columns = ['owner', 'repo_name', 'pull_number', 'url']
            missing_columns = [col for col in required_input_columns if col not in reader.fieldnames]
            if missing_columns:
                print(f"Error: Input CSV '{input_csv_file}' is missing required columns: {', '.join(missing_columns)}")
                return

            writer.writerow(csv_header)

            for row_count, row in enumerate(reader):
                print(f"\n--- Processing CSV row {row_count + 1} ---")
                analyze_pr_from_csv_row(row, writer)
                print(f"--- Finished processing CSV row {row_count + 1}, waiting 1 second ---")
                time.sleep(1)

        print(f"\nAll processing complete. Results have been written to '{output_csv_file}'.")

    except FileNotFoundError:
        print(f"Error: Input CSV file '{input_csv_file}' not found.")
        print(f"Current working directory: {os.getcwd()}")
        print("Please place the CSV file in the same directory as the script, or correct the path in input_csv_file.")
    except Exception as e_main:
        print(f"An unexpected error occurred: {e_main}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
