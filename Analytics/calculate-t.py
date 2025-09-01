import numpy as np
from scipy.stats import chi2_contingency

def perform_chi_squared_test(merged_aapr, total_aapr, merged_naapr, total_naapr):
    """
    A chi-square test of independence will be performed using the AAPR and NAAPR approval data

    Args:
        merged_aapr (int): the number of merged APR
        total_aapr (int): the number of total APR
        merged_naapr (int): the number of merged HPR
        total_naapr (int): the number of total HPR
    """

    if total_aapr <= 0 or total_naapr <= 0:
        return
    if merged_aapr < 0 or merged_aapr > total_aapr or \
       merged_naapr < 0 or merged_naapr > total_naapr:
        return

    # calculate not merged percentage
    not_merged_aapr = total_aapr - merged_aapr
    not_merged_naapr = total_naapr - merged_naapr

    observed_table = np.array([
        [merged_aapr, not_merged_aapr],
        [merged_naapr, not_merged_naapr]
    ])

    print("--- contingency table of observed data ---")
    print("          | Merged | Not merged |")
    print(f"AAPR      | {merged_aapr:4d} | {not_merged_aapr:6d} |")
    print(f"NAAPR     | {merged_naapr:4d} | {not_merged_naapr:6d} |")


    try:
        chi2_stat, p_value, dof, expected_freq = chi2_contingency(observed_table)

        print("\n---  chi-squared result ---")
        print(f"  chi-squared statistic: {chi2_stat:.4f}")
        print(f"  p-value: {p_value:.4f}")
        print(f"  degrees of freedom: {dof}")

        alpha = 0.05
        if p_value < alpha:
            print(f"\n  Interpretation: Since the p-value ({p_value:.4f}) is smaller than the significance level ({alpha:.2f}), the null hypothesis is rejected.")
            print("        Therefore, it can be concluded that there is a statistically significant association between the type of PR (with or without AI assistance) and the PR approval result.")
        else:
            print(f"\n  Interpretation: Since the p-value ({p_value:.4f}) is greater than or equal to the significance level ({alpha:.2f}), the null hypothesis is not rejected.")
            print("        Therefore, it cannot be said that there is a statistically significant association between the type of PR (with or without AI assistance) and the PR approval result.")

    except Exception as e:
        print(f"An error occurred while running the chi-square test: {e}")

# AAPR
# merged_aapr_from_image = 475
# total_aapr_from_image = 567
merged_aapr_directly_from_image = 475
total_aapr_directly_from_image = 567

# NAAPR
# merged_naapr_from_image = 521
# total_naapr_from_image = 625
merged_naapr_directly_from_image = 516
total_naapr_directly_from_image = 567
# -------------------------------------------

if __name__ == '__main__':
    perform_chi_squared_test(
        merged_aapr_directly_from_image,
        total_aapr_directly_from_image,
        merged_naapr_directly_from_image,
        total_naapr_directly_from_image
    )