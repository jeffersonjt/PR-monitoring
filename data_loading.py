import os
import json
from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit as st


def load_tables_from_bigquery(project_id, dataset_id, table_names):
    """
    Load multiple BigQuery tables into separate Pandas DataFrames.

    Args:
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_names (list): List of table names to load.

    Returns:
        dict: A dictionary with table names as keys and DataFrames as values.
    """
    dataframes = {}

    for table_name in table_names:
        # Construct the fully qualified table ID
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        # Query the table and load it into a DataFrame
        query = f"SELECT * FROM `{table_id}`"
        df = client.query(query).to_dataframe()
        
        # Store the DataFrame in the dictionary
        dataframes[table_name] = df
        print(f"Loaded table {table_name} into a DataFrame.")

    return dataframes



def process_mentions_table(df, date_col, channel_name):
    """
    Process a mentions table to count mentions per day for the specified channel.

    Args:
        df (pd.DataFrame): The DataFrame containing mentions data.
        date_col (str): The name of the column containing dates.
        channel_name (str): The name of the channel (e.g., "Twitter").

    Returns:
        pd.DataFrame: A DataFrame with `Date` and mention counts for the channel.
    """
    # Ensure the date column is in datetime format
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[date_col] = df[date_col].dt.tz_localize(None)
    df[date_col] = df[date_col].dt.date

    
    # Filter rows with valid dates and within the last year
    one_year_ago = (datetime.now() - timedelta(days=365)).date()
    now = datetime.now().date()
    df = df[(df[date_col] >= one_year_ago) & (df[date_col] <= now)]
    
    # Group by date and count mentions
    mentions_per_day = df.groupby(date_col).size().reset_index(name=channel_name)
    mentions_per_day.rename(columns={date_col: "Date"}, inplace=True)
    
    return mentions_per_day

def combine_mentions():
    """
    Combine mentions from multiple channels into a single DataFrame.

    Args:
        twitter_df (pd.DataFrame): DataFrame for Twitter mentions.
        news_df (pd.DataFrame): DataFrame for News mentions.
        web_df (pd.DataFrame): DataFrame for Web mentions.

    Returns:
        pd.DataFrame: Combined DataFrame with mentions per day for each channel.
    """
    tables = load_tables_from_bigquery(project_id, dataset_id, table_names)
    tables["web_data"].drop_duplicates(inplace=True)
    tables["twitter_data"].rename(columns={"created_at": "Date"}, inplace=True)
    
    # Process each table
    twitter_mentions = process_mentions_table(tables["twitter_data"], "Date", "Twitter")
    news_mentions = process_mentions_table(tables["news_data"], "Date", "News")
    web_mentions = process_mentions_table(tables["web_data"], "Date", "Blog")
    
    # Create a full date range from 1 year ago to today
    one_year_ago = datetime.now() - timedelta(days=365)
    full_date_range = pd.DataFrame({
        "Date": pd.date_range(start=one_year_ago, end=datetime.now(), freq="D")
    })

    # Ensure all Date columns are of type datetime64[ns]
    full_date_range["Date"] = pd.to_datetime(full_date_range["Date"]).dt.date
    
    # Merge all mentions with the full date range
    combined_df = full_date_range.merge(twitter_mentions, on="Date", how="left")
    combined_df = combined_df.merge(news_mentions, on="Date", how="left")
    combined_df = combined_df.merge(web_mentions, on="Date", how="left")
    
    # Fill missing mention counts with 0
    combined_df.fillna(0, inplace=True)
    combined_df["Twitter"] = combined_df["Twitter"].astype(int)
    combined_df["News"] = combined_df["News"].astype(int)
    combined_df["Blog"] = combined_df["Blog"].astype(int)
    
    return combined_df


def load_data():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pr-project-444202-b7bf6b9bf4f8.json"
    combined_df = combine_mentions()
    return combined_df



google_credentials = st.secrets["google_credentials"]
credentials = service_account.Credentials.from_service_account_info(google_credentials)
client = bigquery.Client(credentials=credentials, project=google_credentials["project_id"])
project_id = "pr-project-444202"  # Replace with your Google Cloud project ID
dataset_id = "pr_project_scraped_data"     # Replace with your BigQuery dataset ID
table_names = ["news_data", "web_data", "twitter_data"]  # Replace with your table names


