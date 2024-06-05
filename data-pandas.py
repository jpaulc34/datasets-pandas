import pandas as pd

def load_large_csv(file_path):
    """
    Loading large csv using pandas
    """
    
    df = pd.read_csv(file_path, chunksize=20000)
    chunks = []

    for chunk in df:
        # Perform processing on each chunk

        # Convert the date to datetime64
        chunk['Subscription Date'] = pd.to_datetime(chunk['Subscription Date'], format='%Y-%m-%d')
        
        # Filter data between two dates
        filtered_chunk = chunk.loc[(chunk['Subscription Date'] >= '2021-01-01')
                            & (chunk['Subscription Date'] < '2022-12-31')]
        
        # print(filtered_chunk)
        chunks.append(filtered_chunk)

    # Concat chunks into a dataframe
    df = pd.concat(chunks, axis=0)
    return df


def summarize_data(df):
    """
    Provides basic summary statistics for the dataframe.
    """
    summary = df.describe()
    return summary

def filter_and_save(df, column_name, myList, output_path):
    """
    Filters dataframe and saves the result to a new CSV file.
    """
    filtered_df = df[df[column_name].isin(myList)]
    filtered_df.to_csv(output_path, index=False)

##

file_path = 'customers-100000.csv'
df = load_large_csv(file_path)

# summary_stats = summarize_data(df)
# print(summary_stats)

output_path = 'filtered_data.csv'
# filter_and_save(df, 'Country', ['Togo', 'Sri Lanka'], output_path)