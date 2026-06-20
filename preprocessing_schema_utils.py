import logging

import pandas as pd


def find_unexpected_source_columns(columns, expected_source_columns):
    known_columns = set(expected_source_columns)
    return {column for column in columns if column not in known_columns}


def ensure_expected_output_columns(df, expected_output_columns, context):
    missing_columns = [
        column for column in expected_output_columns if column not in df.columns
    ]

    if missing_columns:
        logging.warning(
            "%s: missing expected columns after preprocessing: %s. Defaulting them safely.",
            context,
            ", ".join(sorted(missing_columns)),
        )

    for column in missing_columns:
        df[column] = pd.NA

    return df
