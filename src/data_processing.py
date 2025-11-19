"""This module contains functions for processing financial transaction data."""
from pathlib import Path
import pandas as pd

# Get absolute path to the CSV file, relative to THIS file's location
PROJECT_ROOT = Path(__file__).parent.parent
input_file = PROJECT_ROOT/"data"/"raw"/"Umsatzliste_AT943200000014664403.csv"


def load_data() -> pd.DataFrame:
    """Reads and processes the financial transaction data from a CSV file."""
    # Define column names since the CSV has no header
    column_names = ["booking_date", "subject", "execution_date", "amount", "currency", "timestamp"]

    # Read the CSV file
    df = pd.read_csv(input_file, delimiter=";", names=column_names)

    # Convert amount from German format (comma as decimal) to float
    df["amount"] = df["amount"].str.replace('.', '', regex=False)  # Remove thousands separator
    df["amount"] = df["amount"].str.replace(',', '.', regex=False)  # Replace comma with period
    df["amount"] = pd.to_numeric(df["amount"])

    # Convert dates from German format (DD.MM.YYYY)
    df["booking_date"] = pd.to_datetime(df["booking_date"], dayfirst=True)
    df["execution_date"] = pd.to_datetime(df["execution_date"], dayfirst=True)

    return df

def extract_sender_recipient(subject: str) -> str:
    """Extracts sender or recipient information from the subject field."""
    # TODO: Define actual parsing logic based on known patterns in the subject field
    if "sekulic" in subject.lower():
        return "Sekulic"
    else:
        return "N/A"  # Default value if no pattern matches

def transform_file() -> pd.DataFrame:
    """Transforms the input CSV file and saves the processed data."""
    # Load the data and set up path for output file
    df = load_data()
    output_file = PROJECT_ROOT/"data"/"processed"/"processed_transactions.csv"

    # Select needed columns and add new columns to the DataFrame
    df_processed = df[["booking_date", "subject", "amount"]].copy()
    df_processed = df_processed.assign(
        sender_recipient=df_processed['subject'].apply(extract_sender_recipient),
        purpose="",
        category=""
    )

    df_processed.to_csv(output_file, index=False)  # Save processed data without index
    return df_processed


def main():
    """Main function for demonstrating the data loading."""
    # df = load_data()
    # print(df.head())
    df_transformed = transform_file()
    print(df_transformed.head(20))

if __name__ == "__main__":
    main()
