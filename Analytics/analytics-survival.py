import pandas as pd
from lifelines.statistics import logrank_test
from lifelines import KaplanMeierFitter
import matplotlib.pyplot as plt
#　mergeまでの時間におけるlogrank検定

FILE_APR = '../Data/RQ1-APR-MERGED.csv'
FILE_HPR = '../Data/RQ1-HPR-MERGED.csv'

DURATION_COLUMN = 'time_to_merge'

def parse_duration_to_hours(df: pd.DataFrame, column_name: str) -> pd.Series:
    time_deltas = pd.to_timedelta(df[column_name], errors='coerce')

    valid_time_deltas = time_deltas.dropna()

    return valid_time_deltas.dt.total_seconds() / 3600


def format_hours_to_readable(hours: float) -> str:
    if pd.isna(hours):
        return "N/A"

    td = pd.to_timedelta(hours, unit='h')
    days = td.days
    seconds = td.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    if days > 0:
        return f"{days} days, {hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def main():
    print("--- Start merge time analysis ---")

    try:

        df_apr = pd.read_csv(FILE_APR)
        df_hpr = pd.read_csv(FILE_HPR)

        durations_apr = parse_duration_to_hours(df_apr, DURATION_COLUMN)
        durations_hpr = parse_duration_to_hours(df_hpr, DURATION_COLUMN)

        print(f"{FILE_APR}: {len(durations_apr)}valid time data found")
        print(f"{FILE_HPR}: {len(durations_hpr)}valid time data found")

    except FileNotFoundError as e:
        print(f"\n error not found : {e.filename}")
        return

    median_apr_hours = durations_apr.median()
    median_hpr_hours = durations_hpr.median()

    print("\n--- 1. Median Merge Time ---")
    print(f"APR median: {format_hours_to_readable(median_apr_hours)} (about {median_apr_hours:.2f} hour)")
    print(f"HPR median: {format_hours_to_readable(median_hpr_hours)} (about {median_hpr_hours:.2f} hour)")


    print("\n--- 2. longrank ---")

    events_apr = [1] * len(durations_apr)
    events_hpr = [1] * len(durations_hpr)

    results = logrank_test(
        durations_apr,
        durations_hpr,
        event_observed_A=events_apr,
        event_observed_B=events_hpr
    )

    print("Result:")
    results.print_summary()

    p_value = results.p_value
    alpha = 0.05

    if p_value < alpha:
        print(f"p-value ({p_value:.4f}) is less than the significance level ({alpha}), the null hypothesis is rejected.")
        print("Conclusion: There is a statistically significant difference between the merge times of the APR and HPR groups.")
    else:
        print(f"p-value ({p_value:.4f}) is greater than the significance level ({alpha}), the null hypothesis is not rejected.。")
        print("Conclusion: There is no statistically significant difference in merge time between the APR and HPR groups.")

    kmf_apr = KaplanMeierFitter()
    kmf_hpr = KaplanMeierFitter()

    kmf_apr.fit(durations_apr, event_observed=events_apr, label='APR Group')
    kmf_hpr.fit(durations_hpr, event_observed=events_hpr, label='HPR Group')

    plt.figure()
    ax = kmf_apr.plot_survival_function()
    kmf_hpr.plot_survival_function(ax=ax)

    plt.title('Kaplan-Meier Survival Curves for Merge Time')
    plt.xlabel('Time (hours)')
    plt.ylabel('Survival Probability (Not yet merged)')
    plt.grid(True)

    output_filename = 'survival_curves.png'
    plt.savefig(output_filename)
    print(f"\n--- 4. Save graph ---")
    print(f" csv file :'{output_filename}' ")


if __name__ == "__main__":
    main()