import csv
import requests
import time
import os


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

# API base URL
API_BASE_URL = "https://api.github.com"

# Input and output files
INPUT_CSV_PATH = "../Data/RQ1-HPR.csv"
OUTPUT_CSV_PATH = "../Data/commit_details/HPR-commit-details.csv"

OUTPUT_CSV_HEADERS = [
    "owner",
    "repo_name",
    "pull_number",
    "url",
    "state",
    "commit_sha",
    "commit_message",
    "commit_date",
    "pushed_date",
    "changed_files",
    "total_additions",
    "total_deletions",
]


# --- Main Processing ---

def get_pr_and_commit_details(owner, repo, pr_number, url, headers):
    """
    Function to get commit details for a specified PR.
    [After change] Returns a list of data dictionaries, one for each commit.
    """
    output_rows = []

    # --- 1. Get PR information itself (no changes here) ---
    pr_url = f"{API_BASE_URL}/repos/{owner}/{repo}/pulls/{pr_number}"
    try:
        pr_res = requests.get(pr_url, headers=headers, timeout=30)
        pr_res.raise_for_status()
        pr_data = pr_res.json()
        pr_state = pr_data.get("state", "UNKNOWN")
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Failed to get PR information: {e}")
        return None

    # --- ★★★ Change: Pagination support ★★★ ---
    # --- 2. Get a list of all commits in the PR (with pagination support) ---
    all_commits_summary_list = []
    # Set the URL for the first page
    commits_url = f"{pr_url}/commits?per_page=100"

    while commits_url:
        try:
            commits_res = requests.get(commits_url, headers=headers, timeout=30)
            commits_res.raise_for_status()

            # Add the current page's commit list to the overall list
            all_commits_summary_list.extend(commits_res.json())

            # Find the URL for the next page from the response headers
            if 'Link' in commits_res.headers:
                links = requests.utils.parse_header_links(commits_res.headers['Link'])
                next_url_info = next((link for link in links if link.get('rel') == 'next'), None)
                if next_url_info:
                    commits_url = next_url_info['url']
                else:
                    commits_url = None  # End loop if there is no next page
            else:
                commits_url = None  # End loop if there is no Link header

        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Failed while getting the list of commits: {e}")
            return None
    # --- ★★★ End of changes ★★★ ---

    print(f"  -> Detected {len(all_commits_summary_list)} commits. Fetching details...")

    # --- 3. Get detailed information for each commit (no changes here) ---
    for commit_summary in all_commits_summary_list:
        commit_sha = commit_summary["sha"]
        time.sleep(0.5)
        commit_detail_url = f"{API_BASE_URL}/repos/{owner}/{repo}/commits/{commit_sha}"

        try:
            commit_detail_res = requests.get(commit_detail_url, headers=headers, timeout=30)
            commit_detail_res.raise_for_status()
            commit_detail = commit_detail_res.json()

            commit_message = commit_detail["commit"]["message"]
            commit_date = commit_detail["commit"]["committer"]["date"]
            pushed_date = commit_summary["commit"]["committer"]["date"]

            changed_files_list = []
            total_additions_for_commit = 0
            total_deletions_for_commit = 0

            files_data = commit_detail.get("files", [])

            for file_info in files_data:
                changed_files_list.append(file_info["filename"])
                total_additions_for_commit += file_info["additions"]
                total_deletions_for_commit += file_info["deletions"]

            row = {
                "owner": owner,
                "repo_name": repo,
                "pull_number": pr_number,
                "url": url,
                "state": pr_state,
                "commit_sha": commit_sha[:7],
                "commit_message": commit_message,
                "commit_date": commit_date,
                "pushed_date": pushed_date,
                "changed_files": ",".join(changed_files_list),
                "total_additions": total_additions_for_commit,
                "total_deletions": total_deletions_for_commit,
            }
            output_rows.append(row)

        except requests.exceptions.RequestException as e:
            print(f"  [WARN] Failed to get details for commit ({commit_sha[:7]}): {e}")
            continue

    return output_rows


def main():
    """Main execution function"""
    if not GITHUB_TOKEN:
        print("Error: GitHub Personal Access Token is not set.")
        print("Please check your setting.txt file.")
        return

    headers = {
        "Authorization": f"bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    output_dir = os.path.dirname(OUTPUT_CSV_PATH)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    if not os.path.exists(INPUT_CSV_PATH):
        print(f"Error: Input file '{INPUT_CSV_PATH}' not found.")
        return

    try:
        with open(OUTPUT_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=OUTPUT_CSV_HEADERS)
            writer.writeheader()

            with open(INPUT_CSV_PATH, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)

                for i, row in enumerate(reader):
                    owner = row.get("owner")
                    repo_name = row.get("repo_name")
                    pull_number = row.get("pull_number")
                    url = row.get("url")

                    if not all([owner, repo_name, pull_number, url]):
                        print(
                            f"Row {i + 1}: Skipping due to missing required information (owner, repo_name, pull_number, url).")
                        continue

                    print(f"\nProcessing: {owner}/{repo_name} PR #{pull_number}")

                    commit_details = get_pr_and_commit_details(owner, repo_name, pull_number, url, headers)

                    if commit_details:
                        writer.writerows(commit_details)
                        print(f"  -> Done. Wrote {len(commit_details)} rows to CSV.")
                    else:
                        print(f"  -> Could not retrieve data for this PR.")

                    time.sleep(1)

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

    print(f"\n--- All processing is complete ---")
    print(f"Results have been saved to '{OUTPUT_CSV_PATH}'.")


if __name__ == "__main__":
    main()