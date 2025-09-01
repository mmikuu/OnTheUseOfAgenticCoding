import csv
import requests
import time
import random
import os

# --- Configuration ---
# Helper function to load the token from an external file, based on add-pr-times.py
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

# The token is now loaded from an external file for better security.
SETTINGS_FILE = "../setting.txt" # Assuming the script is run from a sub-directory like 'scripts'
GITHUB_TOKEN = load_token(SETTINGS_FILE)
GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

# Updated file paths as per your latest version
INPUT_SUMMARY_CSV_PATH = "./module/unique-author-apr.csv"
OUTPUT_SAMPLED_NON_CLAUDE_PRS_CSV_PATH = "../Data/RQ1-HPR.csv"

# Corrected Column names in INPUT_SUMMARY_CSV_PATH as per your clarification
SUMMARY_REPOSITORY_COL = "Repository"
SUMMARY_AUTHOR_COL = "Author"
SUMMARY_COUNT_COL = "Unique_PR_Count"

# String to identify and exclude Claude-generated PRs
CLAUDE_GENERATED_IDENTIFIER_BODY = "🤖 Generated with Claude Code"

# --- GraphQL Query ---
SEARCH_PRS_QUERY_TEMPLATE = """
query SearchPRs($searchQuery: String!, $first: Int!, $after: String) {
  search(query: $searchQuery, type: ISSUE, first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        ... on PullRequest {
          repository {
            nameWithOwner
            owner { login }
            name
            stargazerCount
          }
          author {
            login
          }
          number
          url
          createdAt
          mergedAt
          state
          changedFiles
          additions
          deletions
          baseRefOid
          headRefOid
          mergeCommit {
            oid
          }
          commits(first: 100) {
            totalCount
            edges {
              node {
                commit {
                  oid
                  committedDate
                }
              }
            }
          }
        }
      }
    }
    issueCount
  }
}
"""


# --- Helper Functions ---
def fetch_github_data_graphql(query, variables):
    headers = {
        "Authorization": f"bearer {GITHUB_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    for attempt in range(3):
        try:
            response = requests.post(GRAPHQL_ENDPOINT, json={"query": query, "variables": variables}, headers=headers,
                                     timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"   ⚠️ Request timed out. Retrying ({attempt + 1}/3)...")
            time.sleep(5 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f"   🚨 HTTP Error during GitHub API call: {e}")
            if e.response is not None:
                print(f"   Response status: {e.response.status_code}")
                print(f"   Response content: {e.response.text[:500]}...")
            if attempt < 2:
                print(f"   Retrying ({attempt + 1}/3)...")
                time.sleep(5 * (attempt + 1))
            else:
                raise
    return None


def parse_pr_node_to_dict(pr_node):
    if not pr_node:
        return None
    repo_info = pr_node.get('repository', {})
    owner_login = repo_info.get('owner', {}).get('login') if repo_info.get('owner') else 'N/A'
    repo_name_val = repo_info.get('name', 'N/A')
    data_dict = {
        'owner': owner_login,
        'repo_name': repo_name_val,
        'url': pr_node.get('url'),
        'pull_number': pr_node.get('number'),
        'created_at': pr_node.get('createdAt'),
        'stars': repo_info.get('stargazerCount', 0),
        'changed_files': pr_node.get('changedFiles', 0),
        'added_lines': pr_node.get('additions', 0),
        'deleted_lines': pr_node.get('deletions', 0),
        'base_commit_sha': pr_node.get('baseRefOid'),
        'pr_final_head_sha': pr_node.get('headRefOid'),
        'merge_commit_sha': pr_node.get('mergeCommit', {}).get('oid') if pr_node.get('mergeCommit') else None,
    }
    commits_data = pr_node.get('commits', {})
    data_dict['total_commits'] = commits_data.get('totalCount', 0)
    first_commit_sha_val = None
    pr_commits_edges = commits_data.get('edges', [])
    if pr_commits_edges:
        try:
            valid_commits = [edge['node']['commit'] for edge in pr_commits_edges if edge.get('node', {}).get('commit')]
            if valid_commits:
                sorted_commits = sorted(valid_commits, key=lambda c: c['committedDate'])
                if sorted_commits:
                    first_commit_sha_val = sorted_commits[0]['oid']
        except (TypeError, KeyError, IndexError) as e:
            print(
                f"     ⚠️ Warning: Could not determine first_commit_sha for PR {data_dict.get('url', 'Unknown URL')} due to commit data issue: {e}")
            first_commit_sha_val = 'N/A'
    data_dict['first_commit_sha'] = first_commit_sha_val
    return data_dict


# --- Main Script Logic ---
def main():
    if not GITHUB_TOKEN:
        print("🚨 Error: GITHUB_TOKEN is not set. Please provide a valid GitHub Personal Access Token in the settings file.")
        return

    print(f"📖 Reading sampling tasks from '{INPUT_SUMMARY_CSV_PATH}'...")
    sampling_tasks = []
    try:
        with open(INPUT_SUMMARY_CSV_PATH, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                current_line_num = reader.line_num  # Get current line number for accurate logging
                try:
                    repo_full_name = row.get(SUMMARY_REPOSITORY_COL)
                    author_name = row.get(SUMMARY_AUTHOR_COL)
                    count_str = row.get(SUMMARY_COUNT_COL)

                    owner = None
                    repo = None

                    if not repo_full_name:
                        print(
                            f"⚠️ Warning (Line {current_line_num}): Missing or empty data in '{SUMMARY_REPOSITORY_COL}' column. Skipping.")
                        continue
                    if not author_name:
                        print(
                            f"⚠️ Warning (Line {current_line_num}): Missing or empty data in '{SUMMARY_AUTHOR_COL}' column. Skipping.")
                        continue
                    if count_str is None or count_str == "":  # Check for None or empty string specifically
                        print(
                            f"⚠️ Warning (Line {current_line_num}): Missing or empty data in '{SUMMARY_COUNT_COL}' column. Skipping.")
                        continue

                    # Parse Repository column
                    if '/' in repo_full_name:
                        parts = repo_full_name.split('/', 1)
                        owner = parts[0].strip()
                        repo = parts[1].strip()
                        if not owner or not repo:
                            print(
                                f"⚠️ Warning (Line {current_line_num}): Invalid format in '{SUMMARY_REPOSITORY_COL}' ('{repo_full_name}'). Owner or repo part is empty. Skipping.")
                            continue
                    else:
                        print(
                            f"⚠️ Warning (Line {current_line_num}): Invalid format in '{SUMMARY_REPOSITORY_COL}' ('{repo_full_name}'). Expected 'owner/repo'. Skipping.")
                        continue

                    author_name = author_name.strip()
                    count = int(count_str)  # Can raise ValueError
                    if count > 0:
                        sampling_tasks.append({
                            "owner": owner, "repo": repo, "author": author_name, "count_to_sample": count
                        })
                except ValueError:
                    print(
                        f"⚠️ Warning (Line {current_line_num}): Invalid count value '{count_str}' for author '{author_name}' in repository '{repo_full_name}'. Skipping.")
                    continue
                except Exception as e_row:
                    print(f"⚠️ Error processing row {current_line_num}: {row}. Error: {e_row}. Skipping.")
                    continue
        print(f"Found {len(sampling_tasks)} tasks to process.")
    except FileNotFoundError:
        print(f"🚨 Error: Input file '{INPUT_SUMMARY_CSV_PATH}' not found.")
        return
    except Exception as e_file:
        print(f"🚨 Error reading input CSV '{INPUT_SUMMARY_CSV_PATH}': {e_file}")
        return

    if not sampling_tasks:
        print("No sampling tasks found. Exiting.")
        return

    all_sampled_prs_data = []
    output_pr_identifiers_set = set()

    print("\n🚀 Starting PR sampling process...")
    for i, task in enumerate(sampling_tasks):
        owner, repo, author, num_to_sample = task["owner"], task["repo"], task["author"], task["count_to_sample"]
        print(
            f"\n🔄 Processing task {i + 1}/{len(sampling_tasks)}: Sample {num_to_sample} PRs for author '{author}' in repo '{owner}/{repo}'")

        escaped_claude_identifier = CLAUDE_GENERATED_IDENTIFIER_BODY.replace('"', '\\"')
        search_query_string = (
            f'repo:"{owner}/{repo}" is:pr is:closed author:"{author}" '
            f'NOT "{escaped_claude_identifier}" in:body created:2024-04-30..2025-04-30'  # Date range corrected here
        )

        candidate_prs_for_task = []
        cursor = None
        max_candidates_to_fetch = min(max(num_to_sample * 3, 50), 200)
        page_fetch_size = 50

        print(
            f"   Searching for candidate PRs (target pool: up to {max_candidates_to_fetch}, query: {search_query_string})...")
        pages_fetched = 0
        collected_this_task_count = 0

        try:
            while collected_this_task_count < max_candidates_to_fetch:
                pages_fetched += 1
                print(f"   Fetching page {pages_fetched} (currently {collected_this_task_count} candidates)...")

                remaining_to_fetch_for_pool = max_candidates_to_fetch - collected_this_task_count
                current_page_size = min(page_fetch_size, remaining_to_fetch_for_pool)
                if current_page_size <= 0: break

                variables = {"searchQuery": search_query_string, "first": current_page_size, "after": cursor}
                raw_response = fetch_github_data_graphql(SEARCH_PRS_QUERY_TEMPLATE, variables)

                if not raw_response:
                    print(f"   🚨 Failed to fetch data for page {pages_fetched} after retries.")
                    break

                data = raw_response.get("data", {}).get("search", {})

                if not data.get("edges"):
                    if pages_fetched == 1:
                        print(f"   No PRs found matching criteria for {author} in {owner}/{repo}.")
                    else:
                        print("   No more PRs found for this query.")
                    break

                for edge in data["edges"]:
                    pr_node = edge.get("node")
                    if pr_node:
                        parsed_pr = parse_pr_node_to_dict(pr_node)
                        if parsed_pr:
                            candidate_prs_for_task.append(parsed_pr)
                            collected_this_task_count += 1
                            if collected_this_task_count >= max_candidates_to_fetch: break

                if not data.get("pageInfo", {}).get(
                        "hasNextPage") or collected_this_task_count >= max_candidates_to_fetch:
                    print(f"   Reached end of search results or collected {collected_this_task_count} candidates.")
                    break
                cursor = data["pageInfo"]["endCursor"]
                time.sleep(1.5)
        except requests.exceptions.RequestException:
            print(f"   Skipping task for {author} in {owner}/{repo} due to API error.")
            continue
        except Exception as e:
            print(f"   🚨 An unexpected error occurred while fetching PRs for {owner}/{repo} by {author}: {e}")
            time.sleep(5)
            continue

        if not candidate_prs_for_task:
            print(f"   ⚠️ No non-Claude PRs found for author '{author}' in '{owner}/{repo}' after search.")
            continue

        print(f"   Found {len(candidate_prs_for_task)} candidate PRs. Attempting to sample {num_to_sample}.")

        if len(candidate_prs_for_task) < num_to_sample:
            print(f"   ⚠️ Only {len(candidate_prs_for_task)} found, sampling all.")
            sampled_for_this_task = candidate_prs_for_task
        else:
            sampled_for_this_task = random.sample(candidate_prs_for_task, num_to_sample)

        print(f"   Successfully sampled {len(sampled_for_this_task)} PRs for this task.")

        for pr_data in sampled_for_this_task:
            pr_key_owner = str(pr_data.get('owner', 'N/A_owner')).lower()  # Use default if owner is None
            pr_key_repo = str(pr_data.get('repo_name', 'N/A_repo')).lower()  # Use default if repo_name is None
            pr_key_number = pr_data.get('pull_number')

            if pr_key_number is None:
                print(f"   ⚠️ Skipping PR with missing pull_number: {pr_data.get('url', 'Unknown URL')}")
                continue

            pr_key = (pr_key_owner, pr_key_repo, pr_key_number)

            if pr_key not in output_pr_identifiers_set:
                all_sampled_prs_data.append(pr_data)
                output_pr_identifiers_set.add(pr_key)
            else:
                print(f"   ℹ️ Note: PR {pr_key_owner}/{pr_key_repo}#{pr_key_number} was already sampled. Skipping.")
        time.sleep(2)

    print(
        f"\n💾 Writing {len(all_sampled_prs_data)} unique sampled PRs to '{OUTPUT_SAMPLED_NON_CLAUDE_PRS_CSV_PATH}'...")
    if not all_sampled_prs_data:
        print("No data to write to CSV.")
        return

    fieldnames = [
        'owner', 'repo_name', 'url', 'pull_number', 'created_at', 'stars',
        'total_commits', 'changed_files', 'added_lines', 'deleted_lines',
        'first_commit_sha', 'merge_commit_sha', 'base_commit_sha', 'pr_final_head_sha'
    ]

    processed_data_for_csv = []
    for row_data in all_sampled_prs_data:
        processed_row = {key: row_data.get(key) for key in fieldnames}
        processed_data_for_csv.append(processed_row)

    try:
        with open(OUTPUT_SAMPLED_NON_CLAUDE_PRS_CSV_PATH, mode='w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_data_for_csv)
        print(f"✅ Successfully wrote results to '{OUTPUT_SAMPLED_NON_CLAUDE_PRS_CSV_PATH}'.")
    except Exception as e:
        print(f"🚨 Error writing to CSV: {e}")
        if processed_data_for_csv:
            print(f"   First item keys (for debugging): {processed_data_for_csv[0].keys()}")


if __name__ == "__main__":
    # random.seed(42) # For reproducible sampling during testing
    main()