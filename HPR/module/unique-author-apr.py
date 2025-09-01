import csv
from collections import defaultdict


def analyze_unique_prs_by_repo_and_author(csv_file_paths, owner_col, repo_col, pr_num_col, author_col):
    # { 'owner/repo_name': { 'author_login': set(pr_number) } }
    repo_author_unique_prs_sets = defaultdict(lambda: defaultdict(set))

    total_rows_processed = 0

    for file_path in csv_file_paths:
        print(f"Loading '{file_path}'...")
        try:
            with open(file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)

                # Check if required columns exist
                required_cols = [owner_col, repo_col, pr_num_col, author_col]
                # Add a check for when reader.fieldnames is None (e.g., empty file)
                if reader.fieldnames is None:
                    print(
                        f"Warning: Could not read headers from file '{file_path}'. Skipping this file.")
                    continue
                if not all(col in reader.fieldnames for col in required_cols):
                    missing = [col for col in required_cols if col not in reader.fieldnames]
                    print(
                        f"Warning: Required columns {missing} not found in file '{file_path}'. Skipping this file.")
                    continue

                for row_num, row in enumerate(reader, 1):
                    total_rows_processed += 1
                    try:
                        owner = row.get(owner_col, "").strip()
                        repo_name = row.get(repo_col, "").strip()
                        pr_number_str = row.get(pr_num_col, "").strip()
                        author_login = row.get(author_col, "").strip()

                        if not (owner and repo_name and pr_number_str and author_login):
                            # print(f"Warning: File '{file_path}', line {row_num}: Required information is missing. Skipping.")
                            continue

                        repo_fullname = f"{owner}/{repo_name}"
                        pr_number = int(pr_number_str)

                        repo_author_unique_prs_sets[repo_fullname][author_login].add(pr_number)

                    except ValueError:
                        print(
                            f"Warning: File '{file_path}', line {row_num}: Invalid PR number '{pr_number_str}'. Skipping.")
                    except Exception as e:
                        print(
                            f"Warning: File '{file_path}', line {row_num}: An error occurred during processing: {e}. Skipping.")
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
        except Exception as e:
            print(f"Error: A problem occurred while reading file '{file_path}': {e}")

    print(f"\nProcessed a total of {total_rows_processed} rows of data.")

    final_repo_author_counts = {
        repo: {author: len(prs_set) for author, prs_set in author_map.items()}
        for repo, author_map in repo_author_unique_prs_sets.items()
    }

    return final_repo_author_counts


def print_formatted_results(repo_author_counts):
    """Formats and prints the results."""
    print("\n--- Unique PR Count by Author per Repository ---")
    if not repo_author_counts:
        print("No data available.")
    else:
        for repo, authors_data in sorted(repo_author_counts.items()):
            total_prs_for_repo = sum(authors_data.values())
            print(f"  Repository: {repo} (Total: {total_prs_for_repo} PRs)")
            for author, count in sorted(authors_data.items()):
                print(f"    - Author: {author}: {count} PRs")


# --- Main processing ---
if __name__ == "__main__":
    input_files = [
        "../../data/APR-CLOSED.csv",
        "../../data/APR-MERGED.csv"
    ]

    OWNER_COL = "owner"
    REPO_COL = "repo_name"
    PR_NUM_COL = "pull_number"
    AUTHOR_COL = "author_login"

    repo_author_unique_pr_counts = analyze_unique_prs_by_repo_and_author(
        input_files, OWNER_COL, REPO_COL, PR_NUM_COL, AUTHOR_COL
    )

    print_formatted_results(repo_author_unique_pr_counts)

    # --- Count and display the number of unique repositories ---
    unique_repositories = set(repo_author_unique_pr_counts.keys())
    number_of_unique_repositories = len(unique_repositories)
    print(f"\n--- Unique Repositories ---")
    print(f"Total number of unique repositories: {number_of_unique_repositories}")
    # If necessary, also display the list of unique repository names
    # print("List of unique repositories:")
    # for repo_name_full in sorted(list(unique_repositories)):
    #     print(f"  - {repo_name_full}")
    # ------------------------------------

    output_summary_path = "unique-author-apr.csv"
    try:
        with open(output_summary_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Repository', 'Author', 'Unique_PR_Count'])
            for repo, authors_data in sorted(repo_author_unique_pr_counts.items()):
                for author, count in sorted(authors_data.items()):
                    writer.writerow([repo, author, count])
        print(f"\nSummary results have been output to '{output_summary_path}'.")
    except Exception as e:
        print(f"Error: An error occurred while writing the summary results to '{output_summary_path}': {e}")