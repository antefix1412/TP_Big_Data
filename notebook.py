from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.config import settings


def ensure_directory(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def load_raw_data() -> pd.DataFrame:
    raw_path = Path(settings.raw_bike_file_path)
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw bike file not found: {raw_path}. Run src/get_api.py first."
        )
    return pd.read_parquet(raw_path)


def compute_missing_values(dataframe: pd.DataFrame) -> pd.DataFrame:
    return (
        dataframe.isna()
        .mean()
        .mul(100)
        .round(2)
        .rename("missing_percentage")
        .reset_index()
        .rename(columns={"index": "column_name"})
        .sort_values("missing_percentage", ascending=False)
    )


def compute_numeric_summary(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    numeric = dataframe.select_dtypes(include="number")
    summary = numeric.describe().transpose().reset_index().rename(columns={"index": "column_name"})
    return numeric, summary


def compute_outliers(numeric: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    for column in numeric.columns:
        series = numeric[column].dropna()
        if series.empty:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = series[(series < lower) | (series > upper)]
        rows.append(
            {
                "column_name": column,
                "lower_bound": round(lower, 3),
                "upper_bound": round(upper, 3),
                "outlier_count": int(outliers.count()),
                "outlier_percentage": round((outliers.count() / len(series)) * 100, 2),
            }
        )

    return pd.DataFrame(rows)


def save_correlation(numeric: pd.DataFrame) -> None:
    correlation = numeric.corr(numeric_only=True).round(3)

    if correlation.empty:
        return

    plt.figure(figsize=(8, 6))
    plt.imshow(correlation, cmap="coolwarm", interpolation="nearest")
    plt.colorbar()
    plt.xticks(range(len(correlation.columns)), correlation.columns, rotation=45, ha="right")
    plt.yticks(range(len(correlation.index)), correlation.index)
    plt.title("Correlation Matrix")
    plt.tight_layout()
    plt.savefig(Path(settings.eda_output_dir) / "correlation_matrix.png")
    plt.close()


def save_eda_report(
    dataframe: pd.DataFrame,
    missing: pd.DataFrame,
    summary: pd.DataFrame,
    outliers: pd.DataFrame,
) -> None:
    report_path = Path(settings.eda_output_dir) / "eda_report.md"
    lines = [
        "# EDA Report",
        "",
        f"- Rows: {len(dataframe)}",
        f"- Columns: {len(dataframe.columns)}",
        "",
        "## Columns Layout",
        "",
    ]

    for column_name, dtype in zip(dataframe.columns, dataframe.dtypes):
        non_null_count = int(dataframe[column_name].count())
        lines.append(f"- {column_name}: {dtype} ({non_null_count} non-null)")

    lines.extend(["", "## Missing Values (%)", ""])
    for row in missing.itertuples(index=False):
        lines.append(f"- {row.column_name}: {row.missing_percentage}%")

    lines.extend(["", "## Numeric Summary", ""])
    for row in summary.itertuples(index=False):
        lines.append(
            f"- {row.column_name}: min={getattr(row, 'min', 'N/A')}, "
            f"max={getattr(row, 'max', 'N/A')}, mean={round(getattr(row, 'mean', 0), 3)}"
        )

    lines.extend(["", "## Outliers", ""])
    if outliers.empty:
        lines.append("- No outliers detected.")
    else:
        for row in outliers.itertuples(index=False):
            lines.append(
                f"- {row.column_name}: {row.outlier_count} outliers "
                f"({row.outlier_percentage}%)"
            )

    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ensure_directory(settings.eda_output_dir)
    dataframe = load_raw_data()

    missing = compute_missing_values(dataframe)
    numeric, summary = compute_numeric_summary(dataframe)
    outliers = compute_outliers(numeric)
    save_correlation(numeric)
    save_eda_report(dataframe, missing, summary, outliers)

    print(f"EDA completed. Outputs saved in: {Path(settings.eda_output_dir).resolve()}")
    print(f"Rows: {len(dataframe)}")
    print(f"Columns: {len(dataframe.columns)}")


if __name__ == "__main__":
    main()
