import pandas as pd
import sys

def summarize_with_revision(pr_df):
    """[For PRs with revisions] Aggregates revision and initial metrics from all commits of a single PR."""

    # Split the PR into initial commits and revision commits
    revisions = pr_df[pr_df['Is_first_commit'] == False]
    first_commits = pr_df[pr_df['Is_first_commit'] == True]

    # --- Calculate metrics related to revisions ---
    revision_commit_count = len(revisions)
    revision_adds = revisions['total_additions'].sum()
    revision_dels = revisions['total_deletions'].sum()
    revision_change_lines = revision_adds + revision_dels

    # Calculate the number of unique files newly added in revisions
    first_commit_files_str = ','.join(first_commits['changed_files'].fillna(''))
    first_commit_files = set(f for f in first_commit_files_str.split(',') if f)

    revision_files_str = ','.join(revisions['changed_files'].fillna(''))
    revision_files = set(f for f in revision_files_str.split(',') if f)

    newly_changed_files = revision_files - first_commit_files
    revision_file_change_count = len(newly_changed_files)

    # --- Count the number of Co-authored commits ---
    # Count commits containing "Co-Authored-By: Claude" among revision commits
    co_authored_count = 0
    if 'commit_message' in revisions.columns:
        co_authored_count = revisions['commit_message'].fillna('').str.contains(
            'Co-Authored-By: Claude',
            case=False,  # case-insensitive
            regex=False
        ).sum()

    # --- Calculate metrics related to initial commits ---
    initial_adds = first_commits['total_additions'].sum()
    initial_dels = first_commits['total_deletions'].sum()
    initial_change_lines_count = initial_adds + initial_dels
    initial_file_change_count = len(first_commit_files)

    static_info = pr_df.iloc[0]

    return pd.Series({
        'url': static_info['url'],
        'state': static_info['state'],
        # Metrics for revisions (clarified column names)
        'revision_commit': revision_commit_count,
        'revision_total_additions': revision_adds,
        'revision_total_deletions': revision_dels,
        'revision_total_change_lines': revision_change_lines,
        'revision_file_change': revision_file_change_count,
        'Co-authored commits number': co_authored_count,  # New column
        # Metrics for initial commits (additional)
        'initial_total_additions': initial_adds,
        'initial_total_deletions': initial_dels,
        'initial_change_lines': initial_change_lines_count,
        'initial_file_change': initial_file_change_count
    })


def summarize_without_revision(pr_df):
    """[For PRs without revisions] Aggregates initial commit metrics from all commits of a single PR."""

    first_commits = pr_df

    initial_commit_count = len(first_commits)
    total_adds = first_commits['total_additions'].sum()
    total_dels = first_commits['total_deletions'].sum()
    total_change_lines = total_adds + total_dels

    changed_files_str = ','.join(first_commits['changed_files'].fillna(''))
    initial_file_count = len(set(f for f in changed_files_str.split(',') if f))

    # --- Concatenate commit_message to create the 'text' column ---
    commit_messages = ""
    if 'commit_message' in first_commits.columns:
        # Join messages from multiple commits with a newline character
        commit_messages = '\n'.join(first_commits['commit_message'].fillna(''))

    static_info = pr_df.iloc[0]

    return pd.Series({
        'url': static_info['url'],
        'state': static_info['state'],
        'initial_commit': initial_commit_count,
        'initial_total_additions': total_adds,
        'initial_total_deletions': total_dels,
        'initial_change_lines': total_change_lines,
        'initial_file_change': initial_file_count,
        'text': commit_messages  # Added
    })


def extract_prs_with_revision(df, output_file):
    """Extracts PRs that had revisions after creation, aggregates metrics, and outputs to a file."""
    print(f"Analysis 1: Extracting and aggregating PRs that 'had revisions' after creation...")

    # Identify PRs without revisions to determine those with revisions

    # Condition 1: All commits have Is_first_commit == True
    pr_groups = df.groupby(['owner', 'repo_name', 'pull_number'])['Is_first_commit']
    is_no_revision = pr_groups.all()
    no_revision_pr_keys_condition1 = set(is_no_revision[is_no_revision].index)

    # Condition 2: No commits have Is_first_commit == True, and the total number of commits is 1
    pr_stats = df.groupby(['owner', 'repo_name', 'pull_number']).agg({
        'Is_first_commit': ['count', 'sum']
    })
    pr_stats.columns = ['total_commits', 'first_commit_count']

    condition2_prs = pr_stats[
        (pr_stats['first_commit_count'] == 0) &
        (pr_stats['total_commits'] == 1)
        ]
    no_revision_pr_keys_condition2 = set(condition2_prs.index)

    # Combine the keys of PRs without revisions (Condition 1 OR Condition 2)
    all_no_revision_pr_keys = no_revision_pr_keys_condition1 | no_revision_pr_keys_condition2

    # Get PRs with revisions by excluding those without revisions from all PRs
    all_pr_keys = set(df.set_index(['owner', 'repo_name', 'pull_number']).index)
    revision_pr_keys = all_pr_keys - all_no_revision_pr_keys

    if not revision_pr_keys:
        print(f"-> No PRs with revisions found. '{output_file}' will not be created.")
        return

    # Extract data for the corresponding PRs
    analysis_df = df[df.set_index(['owner', 'repo_name', 'pull_number']).index.isin(revision_pr_keys)]

    result_df = analysis_df.groupby(['owner', 'repo_name', 'pull_number']).apply(summarize_with_revision).reset_index()

    # Display statistics for Co-authored commits
    co_authored_total = result_df['Co-authored commits number'].sum()
    co_authored_prs = (result_df['Co-authored commits number'] > 0).sum()
    print(f"   Total Co-authored commits: {co_authored_total}")
    print(f"   Number of PRs with Co-authored commits: {co_authored_prs} / {len(result_df)}")

    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"-> Processing complete. Aggregated and output {len(result_df)} PRs with revisions.")
    print(f"   Output file: '{output_file}'")


def extract_prs_without_revision(df, output_file):
    """Extracts PRs that had no revisions after creation, aggregates metrics, and outputs to a file."""
    print(f"Analysis 2: Extracting and aggregating PRs that 'had no revisions' after creation...")

    # Condition 1: All commits have Is_first_commit == True
    pr_groups = df.groupby(['owner', 'repo_name', 'pull_number'])['Is_first_commit']
    is_no_revision = pr_groups.all()
    no_revision_pr_keys_condition1 = set(is_no_revision[is_no_revision].index)

    # Condition 2: No commits have Is_first_commit == True, and the total number of commits is 1
    # First, calculate the number of commits and the count of Is_first_commit=True for each PR
    pr_stats = df.groupby(['owner', 'repo_name', 'pull_number']).agg({
        'Is_first_commit': ['count', 'sum']  # count: total commits, sum: number of True values
    })
    pr_stats.columns = ['total_commits', 'first_commit_count']

    # Extract PRs that meet Condition 2
    condition2_prs = pr_stats[
        (pr_stats['first_commit_count'] == 0) &  # 0 Is_first_commit=True
        (pr_stats['total_commits'] == 1)  # total number of commits is 1
        ]
    no_revision_pr_keys_condition2 = set(condition2_prs.index)

    # Combine keys of PRs that meet either condition
    all_no_revision_pr_keys = no_revision_pr_keys_condition1 | no_revision_pr_keys_condition2

    if not all_no_revision_pr_keys:
        print(f"-> No PRs without revisions found. '{output_file}' will not be created.")
        return

    # Extract data for the corresponding PRs
    analysis_df = df[df.set_index(['owner', 'repo_name', 'pull_number']).index.isin(all_no_revision_pr_keys)]

    result_df = analysis_df.groupby(['owner', 'repo_name', 'pull_number']).apply(
        summarize_without_revision).reset_index()

    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"-> Processing complete. Aggregated and output {len(result_df)} PRs without revisions.")
    print(f"   (Condition 1: {len(no_revision_pr_keys_condition1)}, Condition 2: {len(no_revision_pr_keys_condition2)})")
    print(f"   Output file: '{output_file}'")


def analyze_dataset(input_path, dataset_name):
    """Perform a detailed analysis of the dataset."""
    print(f"\n### Detailed Analysis of {dataset_name} Data ###")

    # Load data
    df = pd.read_csv(input_path, dtype={'commit_sha': str})

    # Basic statistics
    total_records = len(df)
    unique_prs = df[['owner', 'repo_name', 'pull_number']].drop_duplicates()
    unique_pr_count = len(unique_prs)

    print(f"Total records: {total_records:,}")
    print(f"Unique PRs (overall): {unique_pr_count:,}")

    # Number of PRs per state
    print("\nNumber of unique PRs by state:")
    for state in sorted(df['state'].unique()):
        state_df = df[df['state'] == state]
        state_pr_count = len(state_df[['owner', 'repo_name', 'pull_number']].drop_duplicates())
        print(f"  {state}: {state_pr_count:,}")

    # Detailed analysis for MERGED state only
    merged_df = df[df['state'] == 'MERGED'].copy()

    # Exclude commits starting with 'Merge branch' to standardize the analysis set
    if 'commit_message' in merged_df.columns:
        original_count = len(merged_df)
        merged_df = merged_df[~merged_df['commit_message'].str.startswith('Merge branch', na=False)]
        excluded_count = original_count - len(merged_df)
        print(f"Excluded {excluded_count} 'Merge branch' commits from the analysis.")
    else:
        print("Warning: 'commit_message' column not found. Merge commits were not excluded.")

    merged_pr_count = len(merged_df[['owner', 'repo_name', 'pull_number']].drop_duplicates())

    print(f"\nNumber of PRs in MERGED state (after excluding merge commits): {merged_pr_count:,}")

    # Process Is_first_commit
    if merged_df['Is_first_commit'].dtype == 'object':
        merged_df['Is_first_commit'] = merged_df['Is_first_commit'].str.lower().map({'true': True, 'false': False})
    merged_df['Is_first_commit'] = merged_df['Is_first_commit'].fillna(False).astype(bool)

    # Classification of with/without revisions
    # Condition 1: All commits have Is_first_commit==True
    pr_groups = merged_df.groupby(['owner', 'repo_name', 'pull_number'])['Is_first_commit']
    is_no_revision_cond1 = pr_groups.all()
    no_revision_cond1_count = is_no_revision_cond1.sum()

    # Condition 2: Is_first_commit count is 0 and commit count is 1
    pr_stats = merged_df.groupby(['owner', 'repo_name', 'pull_number']).agg({
        'Is_first_commit': ['count', 'sum']
    })
    pr_stats.columns = ['total_commits', 'first_commit_count']
    condition2_mask = (pr_stats['first_commit_count'] == 0) & (pr_stats['total_commits'] == 1)
    no_revision_cond2_count = condition2_mask.sum()

    # Check for overlap between Condition 1 and Condition 2
    cond1_prs = set(is_no_revision_cond1[is_no_revision_cond1].index)
    cond2_prs = set(pr_stats[condition2_mask].index)
    overlap = cond1_prs & cond2_prs

    # Total number of PRs without revisions
    no_revision_total = len(cond1_prs | cond2_prs)
    revision_total = merged_pr_count - no_revision_total

    print(f"\nPRs without revision (Condition 1: All commits Is_first_commit==True): {no_revision_cond1_count:,}")
    print(f"PRs without revision (Condition 2: Is_first_commit count is 0 and commit count is 1): {no_revision_cond2_count:,}")
    print(f"Overlap between Condition 1 and 2: {len(overlap):,}")
    print(f"\nTotal PRs without revision: {no_revision_total:,}")
    print(f"Total PRs with revision: {revision_total:,}")
    print(f"Total (Verification): {no_revision_total + revision_total:,} (should match the number of MERGED PRs)")

    # Percentage
    if merged_pr_count > 0:
        print(f"\nPercentage of PRs without revision: {no_revision_total / merged_pr_count * 100:.1f}%")
        print(f"Percentage of PRs with revision: {revision_total / merged_pr_count * 100:.1f}%")

    # Analysis of Co-authored commits (entire dataset)
    if 'commit_message' in df.columns:
        print("\n### Co-authored commits Analysis ###")
        co_authored_mask = df['commit_message'].fillna('').str.contains(
            'Co-Authored-By: Claude',
            case=False,
            regex=False
        )
        co_authored_count = co_authored_mask.sum()
        print(f"Total Co-authored commits: {co_authored_count:,}")

        # Co-authored commits in MERGED state
        merged_co_authored = merged_df['commit_message'].fillna('').str.contains(
            'Co-Authored-By: Claude',
            case=False,
            regex=False
        ).sum()
        print(f"Number of Co-authored commits in MERGED state: {merged_co_authored:,}")


def process_dataset(input_path, output_rev_path, output_no_rev_path):
    """For a given dataset, classifies PRs based on the presence of revisions and outputs the results."""
    try:
        print(f"Loading '{input_path}'...")
        df = pd.read_csv(input_path, dtype={'commit_sha': str})
        print("Loading complete.")
    except FileNotFoundError:
        print(f"Error: Input file '{input_path}' not found.", file=sys.stderr)
        return
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}", file=sys.stderr)
        return

    if df['Is_first_commit'].dtype == 'object':
        df['Is_first_commit'] = df['Is_first_commit'].str.lower().map({'true': True, 'false': False})
    df['Is_first_commit'] = df['Is_first_commit'].fillna(False).astype(bool)
    merged_df = df[df['state'] == 'MERGED'].copy()

    # Exclude commits starting with 'Merge branch' to standardize the analysis set
    if 'commit_message' in merged_df.columns:
        original_count = len(merged_df)
        merged_df = merged_df[~merged_df['commit_message'].str.startswith('Merge branch', na=False)]
        excluded_count = original_count - len(merged_df)
        print(f"Excluded {excluded_count} 'Merge branch' commits from processing.")
    else:
        print("Warning: 'commit_message' column not found. Merge commits were not excluded.")

    extract_prs_with_revision(merged_df.copy(), output_rev_path)
    print("-" * 50)
    extract_prs_without_revision(merged_df.copy(), output_no_rev_path)


def print_summary(apr_rev_path, apr_no_rev_path, hpr_rev_path, hpr_no_rev_path):
    """Display a summary of the processing results."""
    print("\n" + "=" * 80)
    print("Processing Result Summary")
    print("=" * 80)

    for dataset_name, rev_path, no_rev_path in [
        ("APR", apr_rev_path, apr_no_rev_path),
        ("HPR", hpr_rev_path, hpr_no_rev_path)
    ]:
        print(f"\n### {dataset_name} ###")

        # Load only if the file exists
        try:
            rev_df = pd.read_csv(rev_path)
            rev_count = len(rev_df)

            # Detailed statistics for PRs with revisions
            if 'revision_commit' in rev_df.columns:
                total_revision_commits = rev_df['revision_commit'].sum()
                print(f"\nStatistics for PRs with revision:")
                print(f"  Number of PRs with revision: {rev_count:,}")
                print(f"  Total revision commits in PRs with revision: {total_revision_commits:,}")
                print(f"  Average revision commits per PR with revision: {total_revision_commits / rev_count:.2f}")

            # Also display statistics for Co-authored commits
            if 'Co-authored commits number' in rev_df.columns:
                co_authored_total = rev_df['Co-authored commits number'].sum()
                co_authored_prs = (rev_df['Co-authored commits number'] > 0).sum()
                print(f"\n  Statistics for Co-authored commits:")
                print(f"    Total Co-authored commits in PRs with revision: {co_authored_total:,}")
                print(f"    Number of PRs with Co-authored commits: {co_authored_prs:,} / {rev_count:,}")

                if total_revision_commits > 0:
                    co_author_rate = (co_authored_total / total_revision_commits) * 100
                    print(f"    Percentage of Co-authored commits among revision commits: {co_author_rate:.1f}%")

                if co_authored_prs > 0:
                    avg_co_authored = co_authored_total / co_authored_prs
                    print(f"    Average Co-authored commits per PR that has them: {avg_co_authored:.2f}")
        except:
            rev_count = 0

        try:
            no_rev_df = pd.read_csv(no_rev_path)
            no_rev_count = len(no_rev_df)
        except:
            no_rev_count = 0

        total = rev_count + no_rev_count

        print(f"\nOverall Statistics:")
        print(f"  PRs with revision: {rev_count:,}")
        print(f"  PRs without revision: {no_rev_count:,}")
        print(f"  Total: {total:,}")

        if total > 0:
            print(f"  Revision Rate: {rev_count / total * 100:.1f}%")
            print(f"  No Revision Rate: {no_rev_count / total * 100:.1f}%")


# --- Main processing ---
if __name__ == '__main__':
    print("===== Detailed Dataset Analysis =====")

    # Detailed analysis of APR and HPR
    analyze_dataset(
        input_path='../Data/commit_details/APR-commit-details.csv',
        dataset_name='APR'
    )

    analyze_dataset(
        input_path='../Data/commit_details/HPR-commit-details.csv',
        dataset_name='HPR'
    )

    print("\n" + "=" * 80)
    print("===== Starting processing of APR data =====")
    process_dataset(
        input_path='../Data/commit_details/APR-commit-details.csv',
        output_rev_path='../Data/require_revision/APR-require-revision.csv',
        output_no_rev_path='../Data/require_revision/APR-no-require-revision.csv'
    )
    print("===== Processing of APR data complete =====\n")

    print("#" * 60 + "\n")

    print("===== Starting processing of HPR data =====")
    process_dataset(
        input_path='../Data/commit_details/HPR-commit-details.csv',
        output_rev_path='../Data/require_revision/HPR-require-revision.csv',
        output_no_rev_path='../Data/require_revision/HPR-no-require-revision.csv'
    )
    print("===== Processing of HPR data complete =====")

    # Display summary
    print_summary(
        apr_rev_path='../Data/require_revision/APR-require-revision.csv',
        apr_no_rev_path='../Data/require_revision/APR-no-require-revision.csv',
        hpr_rev_path='../Data/require_revision/HPR-require-revision.csv',
        hpr_no_rev_path='../Data/require_revision/HPR-no-require-revision.csv'
    )

    # Additional detailed analysis
    print("\n" + "=" * 80)
    print("Detailed Analysis of Co-authored commits in PRs with Revisions")
    print("=" * 80)

    for dataset_name, rev_path in [
        ("APR", '../data/require-revision-pr-for-classification/APR-require-revision.csv'),
        ("HPR", '../data/require-revision-pr-for-classification/HPR-require-revision.csv')
    ]:
        try:
            print(f"\n### Details for {dataset_name} PRs with Revisions ###")
            rev_df = pd.read_csv(rev_path)

            if 'revision_commit' in rev_df.columns and 'Co-authored commits number' in rev_df.columns:
                # Display distribution of Co-authored commits
                co_auth_dist = rev_df['Co-authored commits number'].value_counts().sort_index()
                print("\nDistribution of Co-authored commits count:")
                for count, freq in co_auth_dist.items():
                    if count > 0:  # Exclude 0 from display
                        print(f"  PRs with {count} Co-authored commits: {freq}")

                # Information on maximum values
                max_co_authored = rev_df['Co-authored commits number'].max()
                max_revision = rev_df['revision_commit'].max()
                print(f"\nMaximum Co-authored commits count: {max_co_authored}")
                print(f"Maximum revision commits count: {max_revision}")

        except Exception as e:
            print(f"Error: {e}")