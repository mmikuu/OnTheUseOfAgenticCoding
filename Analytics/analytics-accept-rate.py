import pandas as pd
import requests
import time
import os
from datetime import datetime
from tqdm import tqdm
import argparse
#　direct mergeやmergeの数を計算してコンソールに出している

API_BASE_URL = "https://api.github.com"

def load_github_token(settings_file="../../setting.txt"):
    """Load GitHub token from a settings file."""
    try:
        with open(settings_file, 'r', encoding='utf-8') as f:
            return f.readline().strip()
    except FileNotFoundError:
        print(f"Warning: Token file '{settings_file}' not found. Please be aware of API rate limits.")
        return None


def _calculate_inferred_push_dates(commits: list) -> dict:
    """Group commits by time and author to infer the push_date for each commit."""
    if not commits:
        return {}
    try:
        sorted_commits = sorted(commits, key=lambda c: c['commit']['committer']['date'])
    except (KeyError, TypeError):
        print("\n[WARN] Could not sort commits due to incomplete committer date data.")
        sorted_commits = commits

    commit_groups, current_group = [], []
    for commit in sorted_commits:
        if not all(k in commit.get('commit', {}) for k in ['committer']) or not all(
                k in commit['commit']['committer'] for k in ['email', 'date']):
            continue
        commit_date = datetime.fromisoformat(commit['commit']['committer']['date'].replace('Z', '+00:00'))
        if not current_group:
            current_group = [commit]
        else:
            last_commit = current_group[-1]
            last_date = datetime.fromisoformat(last_commit['commit']['committer']['date'].replace('Z', '+00:00'))
            # Group commits if they are from the same author within a 5-minute window
            if (commit['commit']['committer']['email'] == last_commit['commit']['committer']['email'] and abs(
                    (commit_date - last_date).total_seconds()) < 300):
                current_group.append(commit)
            else:
                commit_groups.append(current_group)
                current_group = [commit]
    if current_group:
        commit_groups.append(current_group)

    sha_to_push_date_map = {}
    for group in commit_groups:
        # The push date for a group is the date of the last commit in that group
        push_date_for_group = group[-1]['commit']['committer']['date']
        for commit in group:
            if commit.get('sha'):
                sha_to_push_date_map[commit['sha']] = push_date_for_group
    return sha_to_push_date_map


def update_commit_dates(file_path: str, token: str):
    """(Step 1) Fetch dates from the GitHub API and update the CSV file."""
    print("--- Step 1: Starting Date Fetch and Update ---")
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return

    headers = {"Authorization": f"bearer {token}", "Accept": "application/vnd.github.v3+json"}
    df = pd.read_csv(file_path, dtype=str)

    date_map = {}
    unique_prs = df[['owner', 'repo_name', 'pull_number']].drop_duplicates()
    print(f"Found {len(unique_prs)} unique PRs to update. Fetching latest dates from API...")

    for _, pr in tqdm(unique_prs.iterrows(), total=unique_prs.shape[0], desc="[Step 1] Processing PRs"):
        owner, repo, pr_number = pr['owner'], pr['repo_name'], pr['pull_number']
        all_commits_summary = []
        url = f"{API_BASE_URL}/repos/{owner}/{repo}/pulls/{pr_number}/commits?per_page=100"
        while url:
            try:
                res = requests.get(url, headers=headers, timeout=30)
                res.raise_for_status()
                all_commits_summary.extend(res.json())
                links = requests.utils.parse_header_links(res.headers.get('Link', ''))
                url = next((link['url'] for link in links if link.get('rel') == 'next'), None)
            except requests.exceptions.RequestException as e:
                print(f"\n[WARN] Failed to fetch commits for {owner}/{repo} PR#{pr_number}: {e}")
                url = None

        inferred_push_date_map = _calculate_inferred_push_dates(all_commits_summary)
        for commit in all_commits_summary:
            full_sha = commit.get('sha')
            committer_date = commit.get('commit', {}).get('committer', {}).get('date')
            pushed_date = inferred_push_date_map.get(full_sha)
            if full_sha and committer_date and pushed_date:
                # Map the short SHA to the dates
                date_map[full_sha[:7]] = {'commit_date': committer_date, 'pushed_date': pushed_date}
        time.sleep(0.5)  # Be respectful to the API rate limit

    print(f"\nFetched {len(date_map)} SHA->date mappings from API. Updating DataFrame...")
    original_commit_dates = df['commit_date'].copy() if 'commit_date' in df.columns else None
    original_pushed_dates = df['pushed_date'].copy() if 'pushed_date' in df.columns else None

    mapped_data = df['commit_sha'].str[:7].map(date_map)
    # Get new dates, handling cases where the map doesn't find a SHA
    new_commit_dates = mapped_data.apply(lambda x: x.get('commit_date') if isinstance(x, dict) else None)
    new_pushed_dates = mapped_data.apply(lambda x: x.get('pushed_date') if isinstance(x, dict) else None)

    # Prioritize new dates, but keep old ones if new data isn't available
    df['commit_date'] = new_commit_dates.combine_first(original_commit_dates)
    df['pushed_date'] = new_pushed_dates.combine_first(original_pushed_dates)

    print(f"Saving updated data back to '{file_path}'...")
    df.to_csv(file_path, index=False, encoding='utf-8')
    print("--- Step 1: Complete ---")


# --- Step 2: Mark Initial Commits ---

def mark_earliest_commits(file_path: str):
    """(Step 2) Set the 'Is_first_commit' flag for commits made before the PR was created."""
    print("\n--- Step 2: Starting Marking of Initial Commits ---")
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return

    df = pd.read_csv(file_path)
    required_columns = ['commit_date', 'pr_created_date']
    if not all(col in df.columns for col in required_columns):
        missing_cols = [col for col in required_columns if col not in df.columns]
        raise ValueError(f"Error: Input CSV is missing required columns: {', '.join(missing_cols)}")

    # Convert to datetime objects for comparison, coercing errors to NaT (Not a Time)
    df['commit_date'] = pd.to_datetime(df['commit_date'], errors='coerce')
    df['pr_created_date'] = pd.to_datetime(df['pr_created_date'], errors='coerce')

    # Drop rows where dates could not be parsed
    df.dropna(subset=['commit_date', 'pr_created_date'], inplace=True)

    # Create the boolean flag
    df['Is_first_commit'] = df['commit_date'] < df['pr_created_date']

    num_marked = df['Is_first_commit'].sum()
    print(f"Marked {num_marked} commits with 'Is_first_commit' = True.")

    # Convert dates back to string format for saving
    df['commit_date'] = df['commit_date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['pr_created_date'] = df['pr_created_date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    print(f"Saving updated data back to '{file_path}'...")
    df.to_csv(file_path, index=False, encoding='utf-8')
    print("--- Step 2: Complete ---")


# --- Step 3: Final Analysis ---

def analyze_final_data(file_path: str):
    """(Step 3) Analyze the final CSV and display statistics."""
    print("\n--- Step 3: Starting Final Analysis ---")
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return

    df = pd.read_csv(file_path)
    pr_keys = ['owner', 'repo_name', 'pull_number']

    required_cols = pr_keys + ['state', 'Is_first_commit']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        print(f"Error: Missing required columns for analysis: {', '.join(missing)}. Please run 'mark' stage first.")
        return

    # === 'Merge branch'コミットを除外 ===
    if 'commit_message' in df.columns:
        # 'Merge branch'で始まるコミットを除外
        original_count = len(df)
        df = df[~df['commit_message'].str.startswith('Merge branch', na=False)]
        excluded_count = original_count - len(df)
        print(f"Excluded {excluded_count} 'Merge branch' commits from analysis.")
    else:
        print("Warning: 'commit_message' column not found. Cannot exclude merge commits.")

    # Get a DataFrame of unique pull requests
    unique_prs_df = df.drop_duplicates(subset=pr_keys)
    total_pr_count = len(unique_prs_df)
    if total_pr_count == 0:
        print("No pull requests found to analyze.")
        return

    # Count 'MERGED' PRs (case-insensitive)
    merged_prs_df = unique_prs_df[unique_prs_df['state'].str.upper() == 'MERGED']
    merged_count = len(merged_prs_df)

    # Count 'CLOSED' (but not merged) PRs
    closed_not_merged_count = unique_prs_df[unique_prs_df['state'].str.upper() == 'CLOSED'].shape[0]

    # Is_first_commitをboolに変換
    df['Is_first_commit'] = df['Is_first_commit'].astype(bool)

    # MERGEDのみのデータフレームを作成
    merged_df = df[df['state'].str.upper() == 'MERGED']

    # === 新条件を追加した修正なしPRの判定 ===

    # 条件1: 全てのコミットがIs_first_commit == Trueの場合
    pr_groups = merged_df.groupby(pr_keys)['Is_first_commit']
    is_no_revision_cond1 = pr_groups.all()
    no_revision_prs_cond1 = set(is_no_revision_cond1[is_no_revision_cond1].index)

    # 条件2: Is_first_commit == Trueが1つもなく、かつコミット数が1の場合
    pr_stats = merged_df.groupby(pr_keys).agg({
        'Is_first_commit': ['count', 'sum']
    })
    pr_stats.columns = ['total_commits', 'first_commit_count']

    condition2_mask = (pr_stats['first_commit_count'] == 0) & (pr_stats['total_commits'] == 1)
    no_revision_prs_cond2 = set(pr_stats[condition2_mask].index)

    # 条件1と条件2の重複を確認
    overlap = no_revision_prs_cond1 & no_revision_prs_cond2

    # 修正なしPRの総数（条件1 OR 条件2）
    all_no_revision_prs = no_revision_prs_cond1 | no_revision_prs_cond2
    no_mod_merged_count = len(all_no_revision_prs)

    # 修正ありPRの数
    with_mod_merged_count = merged_count - no_mod_merged_count

    # === 分析結果の表示 ===

    print("\n[Pull Request Status Analysis]")
    print(f"Total unique PRs: {total_pr_count}")
    print(f"Merged PRs: {merged_count}")
    print(f"    - Merged without modification: {no_mod_merged_count}")
    print(f"        - Condition 1 (all commits with Is_first_commit=True): {len(no_revision_prs_cond1)}")
    print(f"        - Condition 2 (single commit with Is_first_commit=False): {len(no_revision_prs_cond2)}")
    print(f"    - Merged with modification: {with_mod_merged_count}")
    print(f"Closed (but not merged) PRs: {closed_not_merged_count}")

    # コミット数の分布も表示
    commit_count_dist = pr_stats['total_commits'].value_counts().sort_index()
    print(f"\n[Commit Count Distribution for Merged PRs]")
    for commit_count, pr_count in commit_count_dist.items():
        print(f"  {commit_count} commit(s): {pr_count} PRs")

    # 単一コミットPRの詳細分析
    single_commit_prs = pr_stats[pr_stats['total_commits'] == 1]
    if len(single_commit_prs) > 0:
        single_with_first_flag = single_commit_prs[single_commit_prs['first_commit_count'] == 1]
        single_without_first_flag = single_commit_prs[single_commit_prs['first_commit_count'] == 0]
        print(f"\n[Single Commit PR Analysis]")
        print(f"Total single commit PRs: {len(single_commit_prs)}")
        print(f"  - With Is_first_commit=True: {len(single_with_first_flag)}")
        print(f"  - With Is_first_commit=False: {len(single_without_first_flag)}")

    # --- Added Analysis Summary ---
    merged_rate = (merged_count / total_pr_count) * 100 if total_pr_count > 0 else 0
    if merged_count > 0:
        no_mod_within_merged_rate = (no_mod_merged_count / merged_count) * 100
    else:
        no_mod_within_merged_rate = 0.0

    print("\n[Final Summary]")
    print(f"Overall PR Merge Rate: {merged_rate:.2f}%")
    print(f"Percentage of Merged PRs ({merged_count}) that had no modifications: {no_mod_within_merged_rate:.2f}%")

    print("--- Step 3: Complete ---")


# --- Main Execution ---

def main():
    """
    Main function to run the processing pipeline based on command-line arguments.
    """
    parser = argparse.ArgumentParser(description="A pipeline tool to process and analyze GitHub PR commit data.")
    parser.add_argument(
        "stage",
        choices=['fetch', 'mark', 'analyze', 'all'],
        help="Choose the processing stage to run: 'fetch' (get dates via API), 'mark' (flag initial commits), 'analyze' (final analysis), 'all' (run all stages)"
    )
    args = parser.parse_args()

    # --- Define the target files to be processed ---
    # You can add more files to this list in the future
    TARGETS = [
        {
            "name": "HPR",
            "csv_path": "../data/commit_details/HPR-commit-details.csv"
        },
        {
            "name": "APR",
            "csv_path": "../data/commit_details/APR-commit-details.csv"
        }
    ]

    # --- Loop through each target and run the selected stages ---
    for target in TARGETS:
        print(f"\n=======================================================")
        print(f"===== Processing Target: {target['name']} ({target['csv_path']}) =====")
        print(f"=======================================================")

        # Stage 1: Fetch data
        if args.stage in ['fetch', 'all']:
            token = load_github_token()
            if not token:
                print("Error: GitHub token is required. Skipping 'fetch' stage.")
                if args.stage == 'fetch': return
            else:
                try:
                    update_commit_dates(target['csv_path'], token)
                except Exception as e:
                    print(f"An error occurred during Step 1 for {target['name']}: {e}")
                    # Decide if you want to stop or continue with the next target
                    continue

                    # Stage 2: Mark commits
        if args.stage in ['mark', 'all']:
            try:
                mark_earliest_commits(target['csv_path'])
            except Exception as e:
                print(f"An error occurred during Step 2 for {target['name']}: {e}")
                continue

        # Stage 3: Analyze results
        if args.stage in ['analyze', 'all']:
            try:
                analyze_final_data(target['csv_path'])
            except Exception as e:
                print(f"An error occurred during Step 3 for {target['name']}: {e}")
                continue

    print("\nScript processing completed successfully for all targets! 🎉")


if __name__ == "__main__":
    main()