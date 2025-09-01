import pandas as pd
import numpy as np


def clean_col_names(df):
    if df is None:
        return None
    if df.empty:
        return df
    df.columns = df.columns.str.strip().str.lower()
    return df


def create_final_sampled_output_v5(summary_file_path, closed_pr_file_path, merged_pr_file_path, output_file_name):
    try:
        print(f"Loading summary file: {summary_file_path}")
        summary_df = pd.read_csv(summary_file_path)
        summary_df = clean_col_names(summary_df)

        print(f"Loading closed PRs file (for format reference and sampling): {closed_pr_file_path}")
        original_closed_df = pd.read_csv(closed_pr_file_path)
        closed_df_for_sampling = original_closed_df.copy()
        closed_df_for_sampling = clean_col_names(closed_df_for_sampling)

        print(f"Loading merged PRs file (for sampling): {merged_pr_file_path}")
        original_merged_df = pd.read_csv(merged_pr_file_path)
        merged_df_for_sampling = original_merged_df.copy()
        merged_df_for_sampling = clean_col_names(merged_df_for_sampling)

        print("Files loaded successfully.")
        if summary_df is None or summary_df.empty or \
                closed_df_for_sampling is None or \
                merged_df_for_sampling is None:
            print("Error: Summary file is empty or PR data files failed to load.")
            return

        print(f"  Original Closed DF columns (for output format): {original_closed_df.columns.tolist()}")
        if not closed_df_for_sampling.empty:
            print(f"  Cleaned Closed DF columns for sampling: {closed_df_for_sampling.columns.tolist()}")
        else:
            print("  Cleaned Closed DF for sampling is empty.")
        if not merged_df_for_sampling.empty:
            print(f"  Cleaned Merged DF columns for sampling: {merged_df_for_sampling.columns.tolist()}")
        else:
            print("  Cleaned Merged DF for sampling is empty.")
        print(f"  Cleaned Summary DF columns: {summary_df.columns.tolist()}")

    except FileNotFoundError as e:
        print(f"Error: File not found. {e}")
        return
    except Exception as e:
        print(f"An error occurred while loading files: {e}")
        return

    expected_summary_cols = ['repository', 'author', 'unique_pr_count']
    if not all(col in summary_df.columns for col in expected_summary_cols):
        print(f"Error: '{summary_file_path}' is missing required columns {expected_summary_cols}.")
        print(f"Current columns (cleaned): {summary_df.columns.tolist()}")
        return

    pr_unique_id_col_cleaned = 'url'
    author_col_in_prs_cleaned = 'author_login'
    repo_owner_col_cleaned = 'owner'
    repo_name_col_cleaned = 'repo_name'

    print("\nAdding temporary 'full_repo_name_temp' column for matching...")
    for df, df_name in [(merged_df_for_sampling, "merged_df"), (closed_df_for_sampling, "closed_df")]:
        if df is not None and not df.empty:
            if repo_owner_col_cleaned in df.columns and repo_name_col_cleaned in df.columns:
                df['full_repo_name_temp'] = df.apply(
                    lambda row: f"{row[repo_owner_col_cleaned]}/{row[repo_name_col_cleaned]}" if pd.notna(
                        row[repo_owner_col_cleaned]) and pd.notna(row[repo_name_col_cleaned]) else None,
                    axis=1
                )
            else:
                print(
                    f"Warning ({df_name}): '{repo_owner_col_cleaned}' or '{repo_name_col_cleaned}' columns not found. 'full_repo_name_temp' will be created with None.")
                df['full_repo_name_temp'] = None
        elif df is not None and df.empty:
            df['full_repo_name_temp'] = None

    all_sampled_groups = []
    print("\nStarting sampling process based on combined pool per repository/author...")

    for index, summary_row in summary_df.iterrows():
        target_repo = summary_row['repository']
        target_author = summary_row['author']
        try:
            num_to_sample = int(summary_row['unique_pr_count'])
        except ValueError:
            print(
                f"Warning (summary_df row {index}): Repository '{target_repo}', Author '{target_author}' has an invalid unique_pr_count '{summary_row['unique_pr_count']}'. Skipping.")
            continue

        if num_to_sample <= 0:
            continue

        merged_candidates = pd.DataFrame()
        if merged_df_for_sampling is not None and not merged_df_for_sampling.empty and \
                'full_repo_name_temp' in merged_df_for_sampling.columns and \
                author_col_in_prs_cleaned in merged_df_for_sampling.columns:
            merged_candidates = merged_df_for_sampling[
                (merged_df_for_sampling['full_repo_name_temp'] == target_repo) &
                (merged_df_for_sampling[author_col_in_prs_cleaned] == target_author)
                ]

        closed_candidates = pd.DataFrame()
        if closed_df_for_sampling is not None and not closed_df_for_sampling.empty and \
                'full_repo_name_temp' in closed_df_for_sampling.columns and \
                author_col_in_prs_cleaned in closed_df_for_sampling.columns:
            closed_candidates = closed_df_for_sampling[
                (closed_df_for_sampling['full_repo_name_temp'] == target_repo) &
                (closed_df_for_sampling[author_col_in_prs_cleaned] == target_author)
                ]

        combined_candidates = pd.concat([merged_candidates, closed_candidates], ignore_index=True, sort=False)

        if combined_candidates.empty:
            continue

        if pr_unique_id_col_cleaned not in combined_candidates.columns:
            print(
                f"Error (Repo: {target_repo}, Author: {target_author}): The combined candidates are missing the '{pr_unique_id_col_cleaned}' column. Skipping this pair.")
            continue

        unique_combined_prs = combined_candidates.drop_duplicates(subset=[pr_unique_id_col_cleaned], keep='first')

        if unique_combined_prs.empty:
            continue

        actual_sample_size = min(num_to_sample, len(unique_combined_prs))
        if actual_sample_size > 0:
            sampled_group = unique_combined_prs.sample(n=actual_sample_size, random_state=42)
            all_sampled_groups.append(sampled_group)

    if not all_sampled_groups:
        print("No PR groups were sampled. The output file will not be created.")
        return

    final_all_sampled_prs = pd.concat(all_sampled_groups, ignore_index=True, sort=False)
    print(f"\nTotal PRs collected before final de-duplication: {len(final_all_sampled_prs)}")

    if pr_unique_id_col_cleaned in final_all_sampled_prs.columns and not final_all_sampled_prs.empty:
        final_all_sampled_prs = final_all_sampled_prs.drop_duplicates(subset=[pr_unique_id_col_cleaned], keep='first')
        print(f"Total unique PRs after final de-duplication: {len(final_all_sampled_prs)}")

    if 'full_repo_name_temp' in final_all_sampled_prs.columns:
        final_all_sampled_prs = final_all_sampled_prs.drop(columns=['full_repo_name_temp'])
        print("Temporary column 'full_repo_name_temp' removed from final sampled data.")

    print(
        f"\nAdjusting final output columns to match original closed_df format ({len(original_closed_df.columns)} columns)...")

    output_df = pd.DataFrame(columns=original_closed_df.columns)
    map_original_to_cleaned = {orig_col: orig_col.strip().lower() for orig_col in original_closed_df.columns}

    for original_col_name in original_closed_df.columns:
        cleaned_col_name_for_lookup = map_original_to_cleaned[original_col_name]
        if cleaned_col_name_for_lookup in final_all_sampled_prs.columns:
            if len(final_all_sampled_prs) > 0:
                output_df[original_col_name] = final_all_sampled_prs[cleaned_col_name_for_lookup].values
            else:
                output_df[original_col_name] = np.nan
        else:
            output_df[original_col_name] = np.nan

    if final_all_sampled_prs.empty and not output_df.columns.empty:
        pass

    print("Final column adjustment complete.")

    try:
        output_df.to_csv(output_file_name, index=False, encoding='utf-8')
        print(f"\nProcessing complete. Results have been saved to '{output_file_name}'.")
        print(f"Final number of output PRs: {len(output_df)}")
    except Exception as e:
        print(f"An error occurred while writing to the CSV file: {e}")

create_final_sampled_output_v5(
    summary_file_path='./module/unique-author-hpr.csv',
    closed_pr_file_path='../Data/APR-CLOSED.csv',
    merged_pr_file_path='../Data/APR-MERGED.csv',
    output_file_name='../Data/RQ1-APR.csv'
)
