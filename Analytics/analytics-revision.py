import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import mannwhitneyu
from matplotlib.backends.backend_pdf import PdfPages
import warnings

# Median and statistical tests for revision and initial metrics
warnings.filterwarnings('ignore')


def filter_merge_commits(df):
    """
    Exclude commits with messages starting with 'Merge branch'.
    """
    if 'commit_message' in df.columns:
        original_count = len(df)
        df_filtered = df[~df['commit_message'].str.startswith('Merge branch', na=False)].copy()
        excluded_count = original_count - len(df_filtered)
        print(f"  - Excluded 'Merge branch' commits: {excluded_count}")
        return df_filtered
    else:
        print("  - Warning: 'commit_message' column not found.")
        return df


def recalculate_pr_metrics(df):
    """
    Recalculate metrics per PR after excluding merge commits.
    """
    pr_keys = ['owner', 'repo_name', 'pull_number']

    # remove duplicates PR
    print(f"  - Before aggregation: {len(df)} commits")
    unique_prs = df[pr_keys].drop_duplicates()
    print(f"  - Unique PRs: {len(unique_prs)}")

    aggregations = {
        'revision_commit': 'count',
        'revision_total_additions': 'sum',
        'revision_total_deletions': 'sum',
        'revision_file_change': 'sum',
        'initial_total_additions': 'first',
        'initial_total_deletions': 'first',
        'initial_file_change': 'first'
    }

    available_aggs = {k: v for k, v in aggregations.items() if k in df.columns}

    if not available_aggs:
        print("  - Error: No aggregatable metrics found.")
        return df

    pr_aggregated = df.groupby(pr_keys).agg(available_aggs).reset_index()

    print(f"  - After aggregation: {len(pr_aggregated)} PRs")
    for col in ['revision_file_change', 'revision_total_additions', 'revision_total_deletions']:
        if col in pr_aggregated.columns:
            non_null_count = pr_aggregated[col].notna().sum()
            print(f"  - {col}: {non_null_count}")

    if 'revision_total_additions' in pr_aggregated.columns and 'revision_total_deletions' in pr_aggregated.columns:
        pr_aggregated['revision_total_change_lines'] = (
                pr_aggregated['revision_total_additions'] + pr_aggregated['revision_total_deletions']
        )

    if 'initial_total_additions' in pr_aggregated.columns and 'initial_total_deletions' in pr_aggregated.columns:
        pr_aggregated['initial_change_lines'] = (
                pr_aggregated['initial_total_additions'] + pr_aggregated['initial_total_deletions']
        )

    return pr_aggregated


def print_statistics_and_test(df1, df2, metric, name1='HPR', name2='APR'):
    print(f"\n--- Metric Analysis: '{metric}' ---")

    if metric not in df1.columns or metric not in df2.columns:
        print(f"   Error: Metric '{metric}' not found.")
        return

    data1 = df1[metric].dropna()
    data2 = df2[metric].dropna()

    if len(data1) == 0 or len(data2) == 0:
        print("   Analysis skipped due to insufficient data.")
        return

    print(f"  {name1} Statistics:")
    print(f"    Sample count: {len(data1)}")
    print(f"    Mean: {data1.mean():.2f}")
    print(f"    Median: {data1.median():.2f}")
    print(f"    Standard Deviation: {data1.std():.2f}")

    print(f"  {name2} Statistics:")
    print(f"    Sample count: {len(data2)}")
    print(f"    Mean: {data2.mean():.2f}")
    print(f"    Median: {data2.median():.2f}")
    print(f"    Standard Deviation: {data2.std():.2f}")

    # Mann-Whitney U Test
    stat, p_value = mannwhitneyu(data1, data2, alternative='two-sided')
    print("  Mann-Whitney U Test:")
    print(f"    U-statistic: {stat:.4f}")
    print(f"    p-value: {p_value:.4f}")

    if p_value < 0.05:
        print("    => p-value is < 0.05, indicating a statistically significant difference.")
    else:
        print("    => p-value is >= 0.05, indicating no statistically significant difference.")


def create_split_violin_plot(data_apr, data_hpr, metric, title, ylabel, log_scale=True, ax=None):

    df_apr = pd.DataFrame({metric: data_apr, 'Group': 'APR'})
    df_hpr = pd.DataFrame({metric: data_hpr, 'Group': 'HPR'})
    combined_df = pd.concat([df_apr, df_hpr], ignore_index=True)

    print(f"\nGenerating plot for '{title}':")
    print(f"  APR - Samples: {len(data_apr)}, Median: {np.median(data_apr):.2f}")
    print(f"  HPR - Samples: {len(data_hpr)}, Median: {np.median(data_hpr):.2f}")

    combined_df['dummy_x'] = ''

    palette = {'APR': '#E74C3C', 'HPR': '#3498DB'}

    sns.violinplot(
        x='dummy_x',
        y=metric,
        hue='Group',
        data=combined_df,
        ax=ax,
        palette=palette,
        inner='box',
        linewidth=1.5,
        split=True,
        hue_order=['APR', 'HPR'],
        legend=False,
        cut=0
    )

    ax.set_xlabel('')
    ax.set_ylabel(ylabel, fontsize=25)
    ax.set_title(title, fontsize=25, fontweight='bold', pad=20)

    if log_scale:
        ax.set_yscale('symlog')
        ax.set_ylim(bottom=0)
        ax.set_ylabel(f'{ylabel}', fontsize=25)
    else:
        ax.set_ylim(bottom=0)

    ax.set_xticks([])

    ax.grid(True, which='both', axis='y', linestyle=':', alpha=1)
    ax.set_axisbelow(True)

    for spine in ax.spines.values():
        spine.set_linewidth(1.5)
        spine.set_edgecolor('black')

    ax.tick_params(axis='both', width=1.5)

    return ax


def create_pr_revision_comparison_plot(hpr_df, apr_df, pdf_path):
    sns.set_style("whitegrid")

    plt.rcParams['axes.titlesize'] = 22
    plt.rcParams['axes.labelsize'] = 22
    plt.rcParams['xtick.labelsize'] = 20
    plt.rcParams['ytick.labelsize'] = 20
    plt.rcParams['legend.fontsize'] = 14


    with PdfPages(pdf_path) as pdf:
        fig, axes = plt.subplots(1, 3, figsize=(18, 7))
        fig.suptitle('PR Revision Analysis Comparison', fontsize=24, fontweight='bold', y=1.02)

        main_metrics = [
            ('revision_file_change', 'Changed Files', 'Changed Files Count'),
            ('revision_total_additions', 'Sum Additions', 'Added Lines'),
            ('revision_total_deletions', 'Sum Deletions', 'Deleted Lines')
        ]

        for idx, (metric, title, ylabel) in enumerate(main_metrics):
            if metric not in hpr_df.columns or metric not in apr_df.columns:
                continue

            ax = axes[idx]

            hpr_data = hpr_df[metric].dropna()
            apr_data = apr_df[metric].dropna()

            if len(hpr_data) == 0 or len(apr_data) == 0:
                continue

            create_split_violin_plot(
                apr_data.values,
                hpr_data.values,
                metric,
                title,
                ylabel,
                log_scale=True,
                ax=ax
            )

            if idx == 2:
                from matplotlib.patches import Patch
                legend_elements = [
                    Patch(facecolor='#E74C3C', label='APR', alpha=0.8),
                    Patch(facecolor='#3498DB', label='HPR', alpha=0.8)
                ]
                ax.legend(handles=legend_elements, loc='upper right', title='Group',
                          frameon=True, fancybox=True, shadow=True)

        plt.tight_layout()
        pdf.savefig(fig, dpi=300, bbox_inches='tight')
        plt.close()


def create_pr_initial_comparison_plot(hpr_df, apr_df, pdf_path):
    sns.set_style("whitegrid")
    plt.rcParams['font.size'] = 14
    plt.rcParams['axes.titlesize'] = 22
    plt.rcParams['axes.labelsize'] = 22
    plt.rcParams['xtick.labelsize'] = 20
    plt.rcParams['ytick.labelsize'] = 20
    plt.rcParams['legend.fontsize'] = 14


    with PdfPages(pdf_path) as pdf:
        fig, axes = plt.subplots(1, 3, figsize=(18, 7))
        fig.suptitle('PR Initial Analysis Comparison', fontsize=24, fontweight='bold', y=1.02)

        initial_metrics = [
            ('initial_file_change', 'Initial Changed Files', 'Changed Files Count'),
            ('initial_total_additions', 'Initial Sum Additions', 'Added Lines'),
            ('initial_total_deletions', 'Initial Sum Deletions', 'Deleted Lines')
        ]

        for idx, (metric, title, ylabel) in enumerate(initial_metrics):
            if metric not in hpr_df.columns or metric not in apr_df.columns:
                continue

            ax = axes[idx]

            hpr_data = hpr_df[metric].dropna()
            apr_data = apr_df[metric].dropna()

            if len(hpr_data) == 0 or len(apr_data) == 0:
                continue

            create_split_violin_plot(
                apr_data.values,
                hpr_data.values,
                metric,
                title,
                ylabel,
                log_scale=True,
                ax=ax
            )

            if idx == 2:
                from matplotlib.patches import Patch
                legend_elements = [
                    Patch(facecolor='#E74C3C', label='APR', alpha=0.8),
                    Patch(facecolor='#3498DB', label='HPR', alpha=0.8)
                ]
                ax.legend(handles=legend_elements, loc='upper right', title='Group',
                          frameon=True, fancybox=True, shadow=True)

        plt.tight_layout()
        pdf.savefig(fig, dpi=300, bbox_inches='tight')
        plt.close()


def create_revision_counts_plot(hpr_df, apr_df, pdf_path):
    """
    Create a plot for the distribution of revision counts.
    """
    sns.set_style("whitegrid")
    plt.rcParams['font.size'] = 14
    plt.rcParams['axes.titlesize'] = 22
    plt.rcParams['axes.labelsize'] = 22
    plt.rcParams['xtick.labelsize'] = 20
    plt.rcParams['ytick.labelsize'] = 20
    plt.rcParams['legend.fontsize'] = 14

    with PdfPages(pdf_path) as pdf:
        if 'revision_commit' in hpr_df.columns and 'revision_commit' in apr_df.columns:
            fig, ax = plt.subplots(figsize=(8, 7))

            hpr_data = hpr_df['revision_commit'].dropna()
            apr_data = apr_df['revision_commit'].dropna()

            create_split_violin_plot(
                apr_data.values,
                hpr_data.values,
                'revision_commit',
                'Distribution of Revision Counts',
                'Revisions Count',
                log_scale=False,
                ax=ax
            )

            ax.set_ylim(0, 100)

            ax.text(-0.2, -10, 'APR',
                    ha='center', va='center', fontsize=13, fontweight='bold',
                    transform=ax.get_xaxis_transform())
            ax.text(0.2, -10, 'HPR',
                    ha='center', va='center', fontsize=13, fontweight='bold',
                    transform=ax.get_xaxis_transform())


            from matplotlib.patches import Patch
            legend_elements = [
                Patch(facecolor='#E74C3C', label='APR', alpha=0.8),
                Patch(facecolor='#3498DB', label='HPR', alpha=0.8)
            ]
            ax.legend(handles=legend_elements, loc='upper right', title='Group',
                      frameon=True, fancybox=True, shadow=True)

            plt.tight_layout()
            pdf.savefig(fig, dpi=300)
            plt.close()


def create_text_length_plot(hpr_df, apr_df, pdf_path):
    """
    Create a plot for the distribution of commit message lengths.
    """
    sns.set_style("whitegrid")
    plt.rcParams['font.size'] = 14
    plt.rcParams['axes.titlesize'] = 22
    plt.rcParams['axes.labelsize'] = 22
    plt.rcParams['xtick.labelsize'] = 20
    plt.rcParams['ytick.labelsize'] = 20
    plt.rcParams['legend.fontsize'] = 14

    with PdfPages(pdf_path) as pdf:
        if 'text_length' in hpr_df.columns and 'text_length' in apr_df.columns:
            fig, ax = plt.subplots(figsize=(8, 7))

            hpr_data = hpr_df['text_length'].dropna()
            apr_data = apr_df['text_length'].dropna()

            create_split_violin_plot(
                apr_data.values,
                hpr_data.values,
                'text_length',
                'Distribution of Commit Message Lengths',
                'Commit Message Length (chars)',
                log_scale=True, # Log scale suitable for wide-ranging text lengths
                ax=ax
            )

            from matplotlib.patches import Patch
            legend_elements = [
                Patch(facecolor='#E74C3C', label='APR', alpha=0.8),
                Patch(facecolor='#3498DB', label='HPR', alpha=0.8)
            ]
            ax.legend(handles=legend_elements, loc='upper right', title='Group',
                      frameon=True, fancybox=True, shadow=True)

            plt.tight_layout()
            pdf.savefig(fig, dpi=300)
            plt.close()


if __name__ == "__main__":
    print("=== Statistical Analysis of Pull Requests with Revisions ===\n")

    # Loading data for revision metrics (require-revision)
    print("=== Loading data for PRs with revisions ===")
    try:
        hpr_revision_df = pd.read_csv('../Data/require_revision/HPR-require-revision.csv')
        print(f"HPR revision data loaded successfully: {len(hpr_revision_df)} PRs")

        pr_keys = ['owner', 'repo_name', 'pull_number']
        if all(col in hpr_revision_df.columns for col in pr_keys):
            duplicates = hpr_revision_df.duplicated(subset=pr_keys)
            if duplicates.any():
                print(f"  Warning: {duplicates.sum()} duplicate PRs detected.")
                hpr_revision_df = hpr_revision_df.drop_duplicates(subset=pr_keys)
                print(f"  After removing duplicates: {len(hpr_revision_df)} PRs")
    except FileNotFoundError:
        print("Error: HPR-require-revision.csv not found.")
        exit(1)

    try:
        apr_revision_df = pd.read_csv('../Data/require_revision/APR-require-revision.csv')
        print(f"APR revision data loaded successfully: {len(apr_revision_df)} PRs")

        if all(col in apr_revision_df.columns for col in pr_keys):
            duplicates = apr_revision_df.duplicated(subset=pr_keys)
            if duplicates.any():
                print(f"  Warning: {duplicates.sum()} duplicate PRs detected.")
                apr_revision_df = apr_revision_df.drop_duplicates(subset=pr_keys)
                print(f"  After removing duplicates: {len(apr_revision_df)} PRs")
    except FileNotFoundError:
        print("Error: APR-require-revision.csv not found.")
        exit(1)

    # Loading data for metrics related to the first commit (no-require-revision)
    print("\n=== Loading data for PRs without revisions (initial commits) ===")
    try:
        hpr_initial_df = pd.read_csv('../Data/require_revision/HPR-no-require-revision.csv')
        print(f"HPR initial data loaded successfully: {len(hpr_initial_df)} PRs")

        if all(col in hpr_initial_df.columns for col in pr_keys):
            duplicates = hpr_initial_df.duplicated(subset=pr_keys)
            if duplicates.any():
                print(f"  Warning: {duplicates.sum()} duplicate PRs detected.")
                hpr_initial_df = hpr_initial_df.drop_duplicates(subset=pr_keys)
                print(f"  After removing duplicates: {len(hpr_initial_df)} PRs")
    except FileNotFoundError:
        print("Error: HPR-no-require-revision.csv not found.")
        exit(1)

    try:
        apr_initial_df = pd.read_csv('../Data/require_revision/APR-no-require-revision.csv')
        print(f"APR initial data loaded successfully: {len(apr_initial_df)} PRs")

        if all(col in apr_initial_df.columns for col in pr_keys):
            duplicates = apr_initial_df.duplicated(subset=pr_keys)
            if duplicates.any():
                print(f"  Warning: {duplicates.sum()} duplicate PRs detected.")
                apr_initial_df = apr_initial_df.drop_duplicates(subset=pr_keys)
                print(f"  After removing duplicates: {len(apr_initial_df)} PRs")
    except FileNotFoundError:
        print("Error: APR-no-require-revision.csv not found.")
        exit(1)

    # --- Create 'text_length' column ---
    if 'text' in hpr_initial_df.columns:
        hpr_initial_df['text_length'] = hpr_initial_df['text'].fillna('').astype(str).str.len()
        print("\n'text_length' column created for HPR initial data.")
    if 'text' in apr_initial_df.columns:
        apr_initial_df['text_length'] = apr_initial_df['text'].fillna('').astype(str).str.len()
        print("'text_length' column created for APR initial data.")


    column_mapping = {
        'total_additions': 'initial_total_additions',
        'total_deletions': 'initial_total_deletions',
        'total_change_lines': 'initial_change_lines',
        'file_change': 'initial_file_change'
    }

    for old_col, new_col in column_mapping.items():
        if old_col in hpr_initial_df.columns and new_col not in hpr_initial_df.columns:
            hpr_initial_df[new_col] = hpr_initial_df[old_col]

    for old_col, new_col in column_mapping.items():
        if old_col in apr_initial_df.columns and new_col not in apr_initial_df.columns:
            apr_initial_df[new_col] = apr_initial_df[old_col]

    print("\n=== Performing Statistical Analysis ===")

    print("\n--- Analysis of Revision Metrics ---")
    revision_metrics = [
        'revision_commit',
        'revision_total_additions',
        'revision_total_deletions',
        'revision_total_change_lines',
        'revision_file_change'
    ]

    for metric in revision_metrics:
        print_statistics_and_test(hpr_revision_df, apr_revision_df, metric, name1='HPR', name2='APR')

    print("\n--- Analysis of Initial Commit Metrics ---")
    initial_metrics = [
        'initial_total_additions',
        'initial_total_deletions',
        'initial_change_lines',
        'initial_file_change',
        'text_length'  # Added 'text_length'
    ]

    for metric in initial_metrics:
        print_statistics_and_test(hpr_initial_df, apr_initial_df, metric, name1='HPR', name2='APR')

    print("\n" + "=" * 60)

    print("\n=== Creating Violin Plots ===")

    # PDF1: PR Revision Analysis Comparison (3 Key Metrics)
    pdf_path_comparison = '../Data/require_revision/pr_revision_analysis_comparison.pdf'
    create_pr_revision_comparison_plot(hpr_revision_df, apr_revision_df, pdf_path_comparison)
    print(f"\nSaved PR Revision Analysis Comparison to PDF: {pdf_path_comparison}")

    # PDF2: PR Initial Analysis Comparison (Three metrics for the first commit)
    pdf_path_initial = '../Data/require_revision/pr_initial_analysis_comparison.pdf'
    create_pr_initial_comparison_plot(hpr_initial_df, apr_initial_df, pdf_path_initial)
    print(f"\nSaved PR Initial Analysis Comparison to PDF: {pdf_path_initial}")

    # PDF3: Distribution of Revision Counts (revision_commit only)
    pdf_path_revisions = '../Data/require_revision/distribution_of_revision_counts.pdf'
    create_revision_counts_plot(hpr_revision_df, apr_revision_df, pdf_path_revisions)
    print(f"\nSaved Distribution of Revision Counts to PDF: {pdf_path_revisions}")

    # PDF4: Distribution of Commit Message Lengths (text_length only)
    pdf_path_text_length = '../Data/require_revision/distribution_of_text_length.pdf'
    create_text_length_plot(hpr_initial_df, apr_initial_df, pdf_path_text_length)
    print(f"\nSaved Distribution of Commit Message Lengths to PDF: {pdf_path_text_length}")

    print("\nDone")