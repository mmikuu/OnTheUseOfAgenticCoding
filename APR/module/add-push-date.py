import pandas as pd
import requests
import time
import os
from datetime import datetime  # Added for datetime operations
from tqdm import tqdm


# --- Configuration ---
def load_github_token(settings_file="../../setting.txt"):
    """Loads the GitHub token from the settings file."""
    try:
        with open(settings_file, 'r', encoding='utf-8') as f:
            return f.readline().strip()
    except FileNotFoundError:
        print(f"Warning: Token file '{settings_file}' not found.")
        return None


GITHUB_TOKEN = load_github_token()
API_BASE_URL = "https://api.github.com"
CSV_FILE_PATH = "../../Data/commit_details/APR-commit-details.csv"


def _calculate_inferred_push_dates(commits: list) -> dict:
    """
    Groups a commit list by time and author to calculate the inferred push_date for each commit.

    Args:
        commits (list): A list of commit information obtained from the GitHub API.

    Returns:
        dict: A dictionary in the format { 'commit_sha': 'inferred_push_date' }.
    """
    if not commits:
        return {}

    # The API response is not always in chronological order, so sort by committer date
    try:
        sorted_commits = sorted(commits, key=lambda c: c['commit']['committer']['date'])
    except (KeyError, TypeError):
        print("\n[WARN] Could not sort commits because the commit date data is incomplete.")
        sorted_commits = commits

    commit_groups = []
    current_group = []

    for commit in sorted_commits:
        # Handle cases where commit data is incomplete
        if not commit.get('commit') or not commit['commit'].get('committer') or not commit['commit']['committer'].get(
                'email') or not commit['commit']['committer'].get('date'):
            continue  # Skip if necessary keys are missing

        commit_date = datetime.fromisoformat(commit['commit']['committer']['date'].replace('Z', '+00:00'))

        if not current_group:
            current_group = [commit]
        else:
            last_commit = current_group[-1]
            last_date = datetime.fromisoformat(last_commit['commit']['committer']['date'].replace('Z', '+00:00'))

            # If a commit is from the same committer email within 5 minutes, consider it part of the same group
            if (commit['commit']['committer']['email'] == last_commit['commit']['committer']['email'] and
                    abs((commit_date - last_date).total_seconds()) < 300):
                current_group.append(commit)
            else:
                commit_groups.append(current_group)
                current_group = [commit]

    if current_group:
        commit_groups.append(current_group)

    # Create a {commit_sha: push_date} map
    sha_to_push_date_map = {}
    for group in commit_groups:
        # Use the timestamp of the last commit in a group as the push_date for the entire group
        push_date_for_group = group[-1]['commit']['committer']['date']
        for commit in group:
            if commit.get('sha'):
                sha_to_push_date_map[commit['sha']] = push_date_for_group

    return sha_to_push_date_map


def build_date_lookup(df: pd.DataFrame, headers: dict) -> dict:
    """
    Identifies unique PRs from the DataFrame, calls the API, and returns a lookup dictionary
    in the form of { 'short_sha': {'commit_date': committer_date, 'pushed_date': inferred_push_date} }.
    """
    date_map = {}

    unique_prs = df[['owner', 'repo_name', 'pull_number']].drop_duplicates()

    print(f"Found {len(unique_prs)} unique PRs to update. Fetching the latest timestamps from the API...")

    for index, pr in tqdm(unique_prs.iterrows(), total=unique_prs.shape[0], desc="Processing PRs"):
        owner, repo, pr_number = pr['owner'], pr['repo_name'], pr['pull_number']

        all_commits_summary = []
        url = f"{API_BASE_URL}/repos/{owner}/{repo}/pulls/{pr_number}/commits?per_page=100"

        # Fetch all commits for the PR, handling pagination
        while url:
            try:
                res = requests.get(url, headers=headers, timeout=30)
                res.raise_for_status()
                all_commits_summary.extend(res.json())
                if 'Link' in res.headers:
                    links = requests.utils.parse_header_links(res.headers['Link'])
                    next_link = next((link for link in links if link.get('rel') == 'next'), None)
                    url = next_link['url'] if next_link else None
                else:
                    url = None
            except requests.exceptions.RequestException as e:
                print(f"\n[WARN] Failed to fetch commits for {owner}/{repo} PR#{pr_number}: {e}")
                url = None

        # --- New logic starts here ---
        # 1. Create a lookup map for inferred push_dates
        inferred_push_date_map = _calculate_inferred_push_dates(all_commits_summary)

        # 2. Store the final information for each commit in date_map
        for commit in all_commits_summary:
            full_sha = commit.get('sha')
            committer_date = commit.get('commit', {}).get('committer', {}).get('date')

            # Get the inferred pushed_date from the lookup map
            pushed_date = inferred_push_date_map.get(full_sha)

            if full_sha and committer_date and pushed_date:
                # Use the short SHA as a key to store the committer_date and inferred_pushed_date
                date_map[full_sha[:7]] = {
                    'commit_date': committer_date,  # Set committer_date as the commit_date
                    'pushed_date': pushed_date      # Set the inferred push_date as the pushed_date
                }

        # Consider the load on the API
        time.sleep(0.5)

    return date_map


def main():
    """
    Main execution function.
    """
    if not GITHUB_TOKEN:
        print("Error: GitHub token is missing.")
        return

    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: File '{CSV_FILE_PATH}' not found.")
        return

    headers = {"Authorization": f"bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

    print(f"Loading input file '{CSV_FILE_PATH}'...")
    df = pd.read_csv(CSV_FILE_PATH, dtype=str)
    print(f"Loaded a total of {len(df)} rows.")

    # Ensure the target columns exist just in case
    for col in ['commit_date', 'pushed_date']:
        if col not in df.columns:
            df[col] = pd.NA

    # Save the state before making changes
    original_commit_dates = df['commit_date'].copy()
    original_pushed_dates = df['pushed_date'].copy()

    # Create a lookup map of the latest timestamps from the API
    date_map = build_date_lookup(df, headers)
    print(f"\nFetched {len(date_map)} SHA->date mappings from the API.")

    # Logic to update the two columns simultaneously
    print("Updating date columns in the DataFrame...")

    # Use the first 7 characters of the 'commit_sha' column to find the date dictionary from the lookup map
    mapped_data = df['commit_sha'].str[:7].map(date_map)

    # Create new date data (Series)
    new_commit_dates = mapped_data.str.get('commit_date')
    new_pushed_dates = mapped_data.str.get('pushed_date')

    # Use .combine_first() to update only the rows where new data was found
    df['commit_date'] = new_commit_dates.combine_first(original_commit_dates)
    df['pushed_date'] = new_pushed_dates.combine_first(original_pushed_dates)

    # Calculate and display the number of updated rows
    commit_date_updates = (df['commit_date'] != original_commit_dates).sum()
    pushed_date_updates = (df['pushed_date'] != original_pushed_dates).sum() # corrected a typo in pushed_date
    print(f"Number of rows where commit_date was updated: {commit_date_updates}")
    print(f"Number of rows where pushed_date was updated: {pushed_date_updates}")

    print(f"\nOverwriting '{CSV_FILE_PATH}' with the processed results...")
    df.to_csv(CSV_FILE_PATH, index=False, encoding='utf-8')

    print("\nProcessing completed successfully!")


if __name__ == "__main__":
    main()