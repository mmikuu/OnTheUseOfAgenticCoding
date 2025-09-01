import pandas as pd
import sys

COLUMNS_TO_KEEP = ['owner', 'repo_name', 'url', 'state', 'stars']

APR_INPUT_PATH = '../Data/RQ1-APR.csv'
HPR_INPUT_PATH = '../Data/RQ1-HPR.csv'
APR_OUTPUT_PATH = '../Data/purpose/RQ1-APR-classification.csv'
HPR_OUTPUT_PATH = '../Data/purpose/RQ1-HPR-classification.csv'


try:
    apr_df = pd.read_csv(APR_INPUT_PATH)
except FileNotFoundError:
    print(f"Error: 'Not Found {APR_INPUT_PATH}'", file=sys.stderr)
    sys.exit(1)

try:
    hpr_df = pd.read_csv(HPR_INPUT_PATH)
except FileNotFoundError:
    print(f"Error: 'Not Found {HPR_INPUT_PATH}'", file=sys.stderr)
    sys.exit(1)


print(f"'Start {APR_INPUT_PATH}'...")
apr_merged = apr_df[apr_df['state'] == 'MERGED']

apr_sample_size = 213

if len(apr_merged) < apr_sample_size:
    print(f"Warning: The number of MERGED records  ({len(apr_merged)}) is less than the sample size ({apr_sample_size}). Extracting all available records.", file=sys.stderr)
    apr_sampled = apr_merged
else:
    apr_sampled = apr_merged.sample(n=apr_sample_size, random_state=42)

try:
    apr_output_df = apr_sampled[COLUMNS_TO_KEEP]
    apr_output_df.to_csv(APR_OUTPUT_PATH, index=False)
    print(f"'Created {APR_OUTPUT_PATH}' ({len(apr_output_df)}row)")
except KeyError as e:
    print(f"Error: {e} Not found colm '{APR_INPUT_PATH}'", file=sys.stderr)
    sys.exit(1)


print(f"\n'Start {HPR_INPUT_PATH}' ...")

hpr_merged = hpr_df[hpr_df['state'] == 'MERGED']

hpr_sample_size = 221


if len(hpr_merged) < hpr_sample_size:
    print(f"Warnings: The number of MERGED records ({len(hpr_merged)}) is less than the sample size ({hpr_sample_size}). Extracting all available records.", file=sys.stderr)
    hpr_sampled = hpr_merged
else:
    hpr_sampled = hpr_merged.sample(n=hpr_sample_size, random_state=42)

try:
    hpr_output_df = hpr_sampled[COLUMNS_TO_KEEP]
    hpr_output_df.to_csv(HPR_OUTPUT_PATH, index=False)
    print(f"'Created {HPR_OUTPUT_PATH}' ({len(hpr_output_df)}row)")
except KeyError as e:
    print(f"Error: {e} Not found colm '{HPR_INPUT_PATH}'", file=sys.stderr)
    sys.exit(1)

print("\nDone")
