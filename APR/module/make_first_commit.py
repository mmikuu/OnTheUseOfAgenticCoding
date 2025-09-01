import pandas as pd
import os

# --- Configuration ---
CSV_FILE_PATH = "../../Data/commit_details/HPR-commit-details.csv"

def mark_earliest_commit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Marks commits that have a commit_date earlier than the pr_created_date.
    """
    # --- Check for the existence of required columns ---
    required_columns = ['commit_date', 'pr_created_date']
    if not all(col in df.columns for col in required_columns):
        missing_cols = [col for col in required_columns if col not in df.columns]
        # Raise an error to stop processing
        raise ValueError(f"Error: The input CSV is missing required columns. Missing columns: {', '.join(missing_cols)}")

    print("Converting date data to datetime objects...")
    # Convert 'commit_date' and 'pr_created_date' columns to datetime objects (invalid formats will become NaT - Not a Time)
    df['commit_date'] = pd.to_datetime(df['commit_date'], errors='coerce')
    df['pr_created_date'] = pd.to_datetime(df['pr_created_date'], errors='coerce')

    # Remove rows where date conversion failed (NaT)
    df.dropna(subset=['commit_date', 'pr_created_date'], inplace=True)

    print(f"Checking the condition for {len(df)} rows with valid date data...")

    # If 'Is_first_commit' column already exists, reset its values; otherwise, initialize it with False
    if 'Is_first_commit' not in df.columns:
        df['Is_first_commit'] = False
    else:
        print("Resetting values in the 'Is_first_commit' column.")
        df['Is_first_commit'] = False

    # --- ★★★ This is where the new condition is applied ★★★ ---
    # Identify rows where 'commit_date' is earlier than 'pr_created_date'
    condition = df['commit_date'] < df['pr_created_date']

    # Set 'Is_first_commit' to True for the corresponding rows
    df.loc[condition, 'Is_first_commit'] = True

    # Display the number of updated records
    num_marked = df['Is_first_commit'].sum()
    print(f"Marked {num_marked} commits with 'Is_first_commit' = True.")

    # Revert the date format back to the original string format (for compatibility with other processes)
    df['commit_date'] = df['commit_date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['pr_created_date'] = df['pr_created_date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    return df


def main():
    """
    Main execution function.
    """
    print(f"Loading input file '{CSV_FILE_PATH}'...")

    # Check if the input file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: File '{CSV_FILE_PATH}' not found.")
        return

    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(CSV_FILE_PATH)

        # Execute the process to mark the first commits
        processed_df = mark_earliest_commit(df)

        print(f"Overwriting '{CSV_FILE_PATH}' with the processed results...")
        processed_df.to_csv(CSV_FILE_PATH, index=False, encoding='utf-8')

        print("\nProcessing completed successfully! 🎉")

    except Exception as e:
        print(f"\nAn error occurred: {e}")


if __name__ == "__main__":
    main()