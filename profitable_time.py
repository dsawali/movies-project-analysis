import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt

def main():
    wiki = pd.read_json('movies-readable.json.gz', lines=True)
    tomato = pd.read_json('rotten-tomatoes.json.gz', lines=True)
    tomato = tomato.drop(columns='rotten_tomatoes_id')

    joined = wiki.merge(tomato, on='imdb_id', how='inner')
    joined = joined.dropna(subset=['made_profit'])
    joined = joined[joined['made_profit'] == 1]
    joined['publication_date'] = joined['publication_date'].astype(
        str).str[5:7]
    
    data = joined[['publication_date', 'made_profit']]

    grouped = data.groupby('publication_date').agg(
        {'publication_date': ['count']}).reset_index()
    grouped.columns = grouped.columns.get_level_values(1)
    grouped.columns.values[0] = 'month'
    grouped.columns.values[1] = 'count'
    
    x = grouped['month']
    y = grouped['count']

    plt.bar(x, y)
    plt.title('Movies that Profit - Grouped by Publication Month')
    plt.xlabel('Month')
    plt.ylabel('Number of Movies that Profit')
    plt.savefig('month-profit-count')
    # plt.show()


if __name__ == '__main__':
    main()
