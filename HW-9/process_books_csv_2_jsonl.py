import pandas as pd
import json


def combine_features(row):
    """
    Uses only the authors' names as the text content for indexing.
    
    Args:
        row (pd.Series): DataFrame row containing 'authors'.
        
    Returns:
        str: Text containing only the authors' names.
    """
    try:
        return row['authors']
    except KeyError as e:
        print("Error:", e)
        return ""


def process_books_csv(input_file, output_file):
    """
    Processes a books CSV file to create a Vespa-compatible JSON format with authors as text.
    
    Args:
      input_file (str): Path to the input CSV file containing book data.
      output_file (str): Path to the output JSON file in Vespa-compatible format.
    """
    # Load the dataset
    books = pd.read_csv(input_file, on_bad_lines='skip')
    
    # Fill missing values in 'authors' column
    books['authors'] = books['authors'].fillna('')
    
    # Create the "text" column by setting it to authors
    books["text"] = books.apply(combine_features, axis=1)

    # Select and rename columns to match the Vespa format
    books = books[['bookID', 'title', 'text']]
    books.rename(columns={'bookID': 'doc_id'}, inplace=True)

    # Create 'fields' column as a JSON-like structure for each record
    books['fields'] = books.apply(lambda row: row.to_dict(), axis=1)

    # Create 'put' column based on 'doc_id'
    books['put'] = books['doc_id'].apply(lambda x: f"id:hybrid-search:doc::{x}")

    # Select only the final columns for output
    df_result = books[['put', 'fields']]
    
    # Display and save the final processed DataFrame as JSON
    print(df_result.head())
    df_result.to_json(output_file, orient='records', lines=True)


# Example usage of the function
process_books_csv("books.csv", "clean_books.jsonl")
