import requests
import csv
import os
import json
import time

SETTINGS_FILENAME = "../setting.txt"

OPEN_PR_CSV_FILENAME = "../Data/APR-OPEN.csv"
CLOSED_NOT_MERGED_PR_CSV_FILENAME = "../Data/APR-CLOSED.csv"
MERGED_PR_CSV_FILENAME = "../Data/APR-MERGED.csv"

BASE_SEARCH_QUERY_TEMPLATE = '"🤖 Generated with Claude Code" is:pr in:body'

RESULTS_PER_PAGE = 50
GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 2
REQUEST_TIMEOUT = 30

graphql_query = """
query SearchPRs($searchQuery: String!, $first: Int!, $after: String) {
  search(query: $searchQuery, type: ISSUE, first: $first, after: $after) {
    issueCount
    pageInfo {
      endCursor
      hasNextPage
    }
    edges {
      node {
        ... on PullRequest {
          number
          url
          additions
          deletions
          changedFiles
          repository {
            nameWithOwner
            name
            owner {
              login
            }
            stargazerCount
          }
          baseRefOid
          headRefOid
          firstCommit: commits(first: 1) {
            nodes {
              commit {
                oid
              }
            }
          }
          commits {
            totalCount
          }
          mergeCommit {
            oid
          }
        }
      }
    }
  }
}
"""


def load_settings(filename):
    token = None
    date_ranges = []

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if not lines:
                print(f"Error: {filename} is empty.")
                exit()

            token = lines[0].strip()
            if not token or token == "YOUR_GITHUB_TOKEN_HERE" or not token.startswith("ghp_") and not token.startswith("github_pat_"):
                print(f"Error: A valid GitHub token was not found on the first line of {filename}.")
                print("The token must start with 'ghp_' or 'github_pat_'.")
                exit()

            for i, line_content in enumerate(lines[1:], start=2):
                line_content = line_content.strip()
                if line_content:
                    try:
                        start_date, end_date = line_content.split(',')
                        date_ranges.append((start_date.strip(), end_date.strip()))
                    except ValueError:
                        print(f"Error: The date format on line {i} of {filename} is incorrect. It should be 'YYYY-MM-DD,YYYY-MM-DD'.")
                        exit()
    except FileNotFoundError:
        print(f"Error: Settings file '{filename}' not found.")
        print(f"Verify the script's execution location and ensure the path specified in '{SETTINGS_FILENAME}' is correct.")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Please create the settings file in the following format:\nYOUR_GITHUB_TOKEN_HERE\nYYYY-MM-DD,YYYY-MM-DD\n...")
        exit()
    except Exception as e:
        print(f"Error: An unexpected error occurred while reading {filename}: {e}")
        exit()

    if not date_ranges:
        print(f"Error: No valid date ranges found in {filename} (after the token line).")
        exit()

    return token, date_ranges


CSV_COLUMNS = [
    "owner", "repo_name", "url", "pull_number", "stars",
    "total_commits", "changed_files", "added_lines", "deleted_lines",
    "first_commit_sha", "merge_commit_sha",
    "base_commit_sha", "pr_final_head_sha"
]


def write_consolidated_csv(filename, data_list, columns):
    if not data_list:
        print(f"\nNo data to write to file {filename}.")
        return

    output_dir = os.path.dirname(filename)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"Directory {output_dir} created.")
        except OSError as e:
            print(f"Failed to create directory {output_dir}: {e}")
            return

    print(f"\nWriting {len(data_list)} collected items to CSV file {filename}...")
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data_list)
        print(f"Successfully wrote data to {filename}.")
    except IOError as e:
        print(f"Error writing to CSV file {filename}: {e}")


GITHUB_TOKEN, date_ranges_to_process = load_settings(SETTINGS_FILENAME)

headers = {
    "Authorization": f"bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json",
}

pr_statuses_to_process = [
    {"status_filter": "is:open -is:draft", "file_suffix": "open_nodraft"},
    {"status_filter": "is:closed", "file_suffix": "closed"}
]

all_open_prs_overall = []
all_closed_not_merged_prs_overall = []
all_merged_prs_overall = []

for start_date, end_date in date_ranges_to_process:
    for status_info in pr_statuses_to_process:
        current_status_filter = status_info["status_filter"]
        current_file_suffix = status_info["file_suffix"]

        SEARCH_QUERY = f'{BASE_SEARCH_QUERY_TEMPLATE} {current_status_filter} created:{start_date}..{end_date}'

        print(f"\n{'=' * 80}")
        print(f"Processing: Date range {start_date}..{end_date}, Status: {current_file_suffix}")
        print(f"Executing search query: {SEARCH_QUERY}")
        print(f"{'=' * 80}\n")

        current_query_prs_data = []
        current_cursor = None
        page_count = 0
        total_found_for_query = 0
        pagination_loop_active = True

        while pagination_loop_active:
            page_count += 1
            if len(current_query_prs_data) >= 1000:
                print("Reached the API limit of 1000 items, ending retrieval for this query.")
                break

            print(f"\nFetching page {page_count} (Query: {SEARCH_QUERY[:70]}...).")

            variables = {
                "searchQuery": SEARCH_QUERY,
                "first": RESULTS_PER_PAGE,
                "after": current_cursor
            }

            response = None
            data = None
            last_exception = None
            for attempt in range(MAX_RETRIES):
                try:
                    print(f"  Request attempt {attempt + 1}/{MAX_RETRIES}...")
                    response = requests.post(
                        GRAPHQL_ENDPOINT,
                        headers=headers,
                        json={"query": graphql_query, "variables": variables},
                        timeout=REQUEST_TIMEOUT
                    )

                    if 500 <= response.status_code < 600:
                        print(f"  Received server error ({response.status_code}).")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(INITIAL_RETRY_DELAY ** (attempt + 1)); continue
                        else:
                            break
                    if response.status_code == 401:
                        print(f"  Authentication error (401)...")
                        pagination_loop_active = False;
                        break
                    response.raise_for_status()
                    data = response.json()
                    if "errors" in data: print(f"GraphQL API Error: {data['errors']}"); pagination_loop_active = False
                    last_exception = None;
                    print(f"  Request successful...")
                    break
                except requests.exceptions.Timeout as e:
                    last_exception = e; print("  Timeout...")
                except requests.exceptions.HTTPError as e:
                    print(f"  HTTP Error: {e}")
                    last_exception = e
                    if response and response.status_code == 401:
                        print(f"  (Detail: Aborting processing for this query due to authentication error.)")
                        pagination_loop_active = False
                        break
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    print(f"  Request Error: {e}")
                if attempt < MAX_RETRIES - 1 and pagination_loop_active:
                    time.sleep(INITIAL_RETRY_DELAY ** (attempt + 1)); print("  Retrying...")
                else:
                    print("  Max retries reached or unable to continue..."); break

            if not pagination_loop_active: print("Aborting current query processing."); break
            if last_exception is not None: print(
                f"Failed to fetch page: {last_exception}"); pagination_loop_active = False; break
            if data is None: print("Could not retrieve data."); break

            search_results = data.get("data", {}).get("search", {})
            if not search_results: print("No valid search results."); pagination_loop_active = False; break

            if page_count == 1:
                total_found_for_query = search_results.get('issueCount', 0)
                print(f"Total items found for this query (API report): {total_found_for_query}...")
                if total_found_for_query == 0: pagination_loop_active = False

            edges = search_results.get("edges", [])
            page_info = search_results.get("pageInfo", {})

            if not edges and total_found_for_query == 0 and page_count == 1:
                pagination_loop_active = False

            if not edges and page_count > 1 and (not page_info or not page_info.get("hasNextPage")):
                pagination_loop_active = False

            prs_added_this_page = 0
            for edge in edges:
                if len(current_query_prs_data) >= 1000: break
                node = edge.get("node")
                if node and "repository" in node and "number" in node:
                    repo_info = node.get("repository", {})
                    stars = repo_info.get("stargazerCount", 0)
                    if stars < 10: continue

                    owner_info = repo_info.get("owner", {})
                    commits_info = node.get("commits", {})
                    first_commit_nodes = node.get("firstCommit", {}).get("nodes", [])

                    merge_commit_node = node.get("mergeCommit")
                    merge_commit_sha_val = "N/A"
                    if merge_commit_node and merge_commit_node.get("oid"):
                        merge_commit_sha_val = merge_commit_node.get("oid")

                    first_commit_sha_val = "N/A"
                    if first_commit_nodes and first_commit_nodes[0].get("commit"):
                        first_commit_sha_val = first_commit_nodes[0]["commit"].get("oid", "N/A")

                    pr_data = {
                        "owner": owner_info.get("login", "N/A"),
                        "repo_name": repo_info.get("name", "N/A"),
                        "url": node.get("url", "N/A"),
                        "pull_number": node.get("number", "N/A"),
                        "stars": stars,
                        "total_commits": commits_info.get("totalCount", 0),
                        "changed_files": node.get("changedFiles", 0),
                        "added_lines": node.get("additions", 0),
                        "deleted_lines": node.get("deletions", 0),
                        "first_commit_sha": first_commit_sha_val,
                        "merge_commit_sha": merge_commit_sha_val,
                        "base_commit_sha": node.get("baseRefOid", "N/A"),
                        "pr_final_head_sha": node.get("headRefOid", "N/A"),
                    }
                    current_query_prs_data.append(pr_data)
                    prs_added_this_page += 1

            print(
                f"Added {prs_added_this_page} PRs meeting filter criteria on this page. (Cumulative for this query: {len(current_query_prs_data)})")

            if page_info.get("hasNextPage") and len(current_query_prs_data) < 1000:
                current_cursor = page_info.get("endCursor")
            else:
                pagination_loop_active = False

        if current_query_prs_data:
            if current_file_suffix == "open_nodraft":
                all_open_prs_overall.extend(current_query_prs_data)
                print(f"Total open PRs collected (after filtering): {len(all_open_prs_overall)}")
            elif current_file_suffix == "closed":
                for pr_item in current_query_prs_data:
                    if pr_item["merge_commit_sha"] != "N/A":
                        all_merged_prs_overall.append(pr_item)
                    else:
                        all_closed_not_merged_prs_overall.append(pr_item)
                print(f"Total merged PRs collected (after adding from this query, filtered): {len(all_merged_prs_overall)}")
                print(f"Total closed (unmerged) PRs collected (after adding from this query, filtered): {len(all_closed_not_merged_prs_overall)}")

        print(f"\nCompleted: Date range {start_date}..{end_date}, Status: {current_file_suffix}")
        is_last_date_range = (start_date == date_ranges_to_process[-1][0] and end_date == date_ranges_to_process[-1][1])
        is_last_status = (status_info == pr_statuses_to_process[-1])
        if not (is_last_date_range and is_last_status):
            print("Waiting 5 seconds before the next processing set...")
            time.sleep(5)

print(f"\n{'=' * 80}")
print("All data collection processes are complete. Starting to write to consolidated files.")
print(f"{'=' * 80}")

write_consolidated_csv(OPEN_PR_CSV_FILENAME, all_open_prs_overall, CSV_COLUMNS)
write_consolidated_csv(CLOSED_NOT_MERGED_PR_CSV_FILENAME, all_closed_not_merged_prs_overall, CSV_COLUMNS)
write_consolidated_csv(MERGED_PR_CSV_FILENAME, all_merged_prs_overall, CSV_COLUMNS)

print("\nAll processing has completed.")
