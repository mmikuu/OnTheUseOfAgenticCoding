import pandas as pd
import os


#A program for creating a CSV for visual inspection for RQ2's rejection
def create_rejected_csv_from_existing_file(input_path, output_path):
    """
    Load an existing CSV file, extract rows where the 'state' is 'CLOSED',
    and save only 5 specific columns as a new 'rejected' version of the CSV file.
    """

    if not os.path.exists(input_path):
        print(f"  [error] Not found : {input_path}")
        return

    try:
        df = pd.read_csv(input_path, low_memory=False)

        rejected_df = df[df['state'].str.upper() == 'CLOSED'].copy()

        if rejected_df.empty:
            print(f" No data was found for the 'CLOSED' state ")
        else:
            print(f"  -> We extracted {len(rejected_df)}rows of 'CLOSED' data")

        output_columns = ['owner', 'repo_name', 'url', 'pull_number', 'state']
        output_df = rejected_df[output_columns]

        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"  -> create directory: {output_dir}")

        output_df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"  -> Done. The results have been saved to {output_path}")

    except KeyError as e:
        print(f"  [Error] The required columns were not found: {e}")
        print(f"  Please check if all five specified columns exist in the input file '{input_path}'")
    except Exception as e:
        print(f"  [Error] An unexpected error occurred during processing: {e}")


if __name__ == '__main__':

    APR_INPUT_FILE = '../Data/RQ1-APR.csv'
    APR_OUTPUT_FILE = '../Data/reject_commit_details/APR-reject-commit-details.csv'

    create_rejected_csv_from_existing_file(
        input_path=APR_INPUT_FILE,
        output_path=APR_OUTPUT_FILE
    )

    print("\n" + "=" * 60 + "\n")


    HPR_INPUT_FILE = '../Data/RQ1-HPR.csv'
    HPR_OUTPUT_FILE = '../Data/reject_commit_details/HPR-reject-commit-details.csv'

    create_rejected_csv_from_existing_file(
        input_path=HPR_INPUT_FILE,
        output_path=HPR_OUTPUT_FILE
    )

    print("\n--- Done ---")