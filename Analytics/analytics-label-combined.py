import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from itertools import combinations
from collections import Counter, defaultdict
import sys
import os
from datetime import datetime


def analyze_label_combinations_from_csv(csv_file):
    try:
        df = pd.read_csv(csv_file)
        print(f"✅ CSV file '{csv_file}'")
    except FileNotFoundError:
        print(f"❌ error: Not find'{csv_file}'")
        sys.exit(1)
    except Exception as e:
        print(f"❌ error: {e}")
        sys.exit(1)


    pair_counts = Counter()
    label_counts = Counter()
    all_labels = set()
    pair_urls = defaultdict(list)


    label_count_distribution = Counter()
    multi_label_rows = []


    for idx, row in df.iterrows():

        url = str(row.iloc[0]) if pd.notna(row.iloc[0]) else f"Row_{idx}"

        labels = []
        for col in ['label1', 'label2', 'label3', 'label4']:
            if col in df.columns:
                val = row[col]
                if pd.notna(val) and str(val).strip():
                    labels.append(str(val).strip())

        labels = list(set(labels))
        label_count = len(labels)
        label_count_distribution[label_count] += 1
        if label_count > 1:
            multi_label_rows.append({
                'url': url,
                'labels': labels,
                'label_count': label_count
            })

        for label in labels:
            label_counts[label] += 1
            all_labels.add(label)

        if len(labels) >= 2:
            for combo in combinations(sorted(labels), 2):
                pair = tuple(sorted(combo))
                pair_counts[pair] += 1
                pair_urls[pair].append(url)

    return pair_counts, label_counts, sorted(all_labels), len(df), pair_urls, label_count_distribution, multi_label_rows


def save_combination_urls_to_csv(pair_urls, output_dir='label_combined_url'):

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"\n📁 Directory '{output_dir}'")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saved_files = []

    for (label1, label2), urls in sorted(pair_urls.items(), key=lambda x: len(x[1]), reverse=True):
        if urls:
            filename = f"{label1}_{label2}_urls_{timestamp}.csv"
            filepath = os.path.join(output_dir, filename)

            df = pd.DataFrame({
                'URL': urls,
                'Label1': label1,
                'Label2': label2,
                'Combination': f"{label1} + {label2}"
            })

            df.to_csv(filepath, index=False, encoding='utf-8')
            saved_files.append({
                'file': filename,
                'combination': f"{label1} + {label2}",
                'count': len(urls)
            })
            print(f"✅  {filename} ({len(urls)})")

    summary_df = pd.DataFrame(saved_files)
    summary_file = os.path.join(output_dir, f"_summary_{timestamp}.csv")
    summary_df.to_csv(summary_file, index=False)

    print(f"\n💾 {len(saved_files)}, '{output_dir}' ")
    print(f"📋 Summary file: {summary_file}")

    return saved_files


def print_multi_label_statistics(label_count_distribution, multi_label_rows, total_items):

    print("\n📊 analytics multi label:")
    print("=" * 50)

    # ラベル数ごとの分布を表示
    print(f"{'label':<10}{'line':<10}{'percentage':<10}")
    print("-" * 30)

    for label_count in sorted(label_count_distribution.keys()):
        count = label_count_distribution[label_count]
        percentage = (count / total_items * 100)
        print(f"{label_count:<10}{count:<10}{percentage:>6.1f}%")

    # 複数ラベルの統計
    multi_label_count = sum(count for labels, count in label_count_distribution.items() if labels > 1)
    multi_label_percentage = (multi_label_count / total_items * 100) if total_items > 0 else 0

    print(f"\n📌 the number of line have the multi label: {multi_label_count}, ({multi_label_percentage:.1f}%)")
    print(
        f"📌 the number of line have one label: {label_count_distribution.get(1, 0)}, ({(label_count_distribution.get(1, 0) / total_items * 100):.1f}%)")
    print(
        f"📌 the number of line have not label: {label_count_distribution.get(0, 0)}, ({(label_count_distribution.get(0, 0) / total_items * 100):.1f}%)")

    # 最も多くのラベルを持つ行の例
    if multi_label_rows:
        max_labels_row = max(multi_label_rows, key=lambda x: x['label_count'])
        print(f"\n🏷️ most common label: {max_labels_row['label_count']}個")
        print(f"   URL: {max_labels_row['url']}")
        print(f"   label: {', '.join(max_labels_row['labels'])}")

        # ラベル数が多い順に上位5件を表示
        print("\n🔝 most common label (Top 5):")
        sorted_rows = sorted(multi_label_rows, key=lambda x: x['label_count'], reverse=True)[:5]
        for i, row in enumerate(sorted_rows, 1):
            print(f"{i}. {row['label_count']}: {', '.join(row['labels'])}")
            print(f"   URL: {row['url']}")


def display_ranking_with_urls(pair_counts, pair_urls, top_n=20, show_urls=3):
    """
    組み合わせのランキングを表示（URL例付き）
    """
    print("\n" + "=" * 80)
    print("🏆 Label combination ranking (Top {})".format(top_n))
    print("=" * 80)

    total_pairs = sum(pair_counts.values())

    if total_pairs == 0:
        print("⚠️  not found")
        return

    for rank, ((label1, label2), count) in enumerate(pair_counts.most_common(top_n), 1):
        percentage = (count / total_pairs * 100)
        print(f"\n{rank}. {label1} + {label2}")
        print(f"   number of times: {count} ({percentage:.1f}%)")

        # URL例を表示
        if (label1, label2) in pair_urls:
            urls = pair_urls[(label1, label2)]
            print(f"   URL (first{min(show_urls, len(urls))}):")
            for i, url in enumerate(urls[:show_urls]):
                print(f"     - {url}")
            if len(urls) > show_urls:
                print(f"     ... other {len(urls) - show_urls} ")


def create_heatmap(pair_counts, all_labels, save_path='label_heatmap.png'):

    if not all_labels:
        print("⚠️  not found the label, can't create heatmap")
        return None


    n_labels = len(all_labels)

    matrix = np.zeros((n_labels, n_labels))

    label_to_idx = {label: idx for idx, label in enumerate(all_labels)}

    for (label1, label2), count in pair_counts.items():
        if label1 in label_to_idx and label2 in label_to_idx:
            idx1 = label_to_idx[label1]
            idx2 = label_to_idx[label2]
            matrix[idx1, idx2] = count
            matrix[idx2, idx1] = count


    plt.figure(figsize=(max(12, n_labels * 0.7), max(10, n_labels * 0.6)))


    mask = np.triu(np.ones_like(matrix, dtype=bool), k=1)


    ax = sns.heatmap(matrix,
                     mask=mask,
                     annot=True,
                     fmt='g',
                     cmap='YlOrRd',
                     xticklabels=all_labels,
                     yticklabels=all_labels,
                     square=True,
                     linewidths=0.5,
                     cbar_kws={"shrink": 0.8, "label": "Count"})

    plt.title('Label Combination Heatmap', fontsize=16, pad=20)
    plt.xlabel('Labels', fontsize=12)
    plt.ylabel('Labels', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()

    # 画像を保存
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"\n🖼️  create the heatmap '{save_path}' ")

    return plt


def save_main_results_to_csv(pair_counts, pair_urls, output_file='label_combinations_main_result.csv'):

    if not pair_counts:
        print("⚠️  not found ")
        return

    # DataFrameに変換
    results = []
    total_pairs = sum(pair_counts.values())

    for (label1, label2), count in pair_counts.most_common():
        percentage = (count / total_pairs * 100) if total_pairs > 0 else 0
        url_count = len(pair_urls.get((label1, label2), []))

        results.append({
            'Label1': label1,
            'Label2': label2,
            'Combination': f'{label1} + {label2}',
            'Count': count,
            'Percentage': round(percentage, 2),
            'URL_Count': url_count
        })

    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"💾 create result  '{output_file}' ")


def print_label_summary(label_counts):
    print("\n📋 Occurrence count by label:")
    print("-" * 40)
    print(f"{'label':<20}{'number of times':<10}")
    print("-" * 40)

    for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{label:<20}{count:<10}")


def main(csv_file):


    pair_counts, label_counts, all_labels, total_items, pair_urls, label_count_distribution, multi_label_rows = analyze_label_combinations_from_csv(
        csv_file)

    print(f"\n📈 Done")
    print(f"- total lines）: {total_items}")
    print(f"- unique label: {len(all_labels)}")
    print(f"- label list: {', '.join(all_labels)}")


    print_label_summary(label_counts)


    print_multi_label_statistics(label_count_distribution, multi_label_rows, total_items)


    display_ranking_with_urls(pair_counts, pair_urls, top_n=20, show_urls=3)


    saved_files = save_combination_urls_to_csv(pair_urls)


    save_main_results_to_csv(pair_counts, pair_urls)


    plt_obj = create_heatmap(pair_counts, all_labels)

    if plt_obj:
        plt_obj.show()

    print("\n✨ Done")


if __name__ == "__main__":

    if len(sys.argv) < 2:
        csv_file = '../Data/classification/Purpose-APR(RQ1).csv'
        print(f" use this file: '{csv_file}'")
    else:
        csv_file = sys.argv[1]

    main(csv_file)