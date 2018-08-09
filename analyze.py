import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt

def main():
    wiki = pd.read_json('movies-readable.json.gz', lines=True)
    tomato = pd.read_json('rotten-tomatoes.json.gz', lines=True)
    tomato = tomato.drop(columns='rotten_tomatoes_id')

    joined = wiki.merge(tomato, on='imdb_id', how='inner')
    joined['publication_date'] = joined['publication_date'].astype(str).str[:4]
    joined['publication_date'] = joined['publication_date'].astype(int)

    ''' ------------------------------------------------------------ '''
    # Count number of movies played and average ratings of cast members


    data = joined[['cast_member', 'audience_average', 'critic_average']]
    
    data = data.dropna()

    exploded = data.cast_member.apply(
        pd.Series).stack().reset_index(
        level=1, drop=True).to_frame('cast_member')
    df = exploded.join(data['audience_average'])
    df = df.join(data['critic_average'])


    averages = df.groupby(['cast_member']).agg(
        {'cast_member': ['count'], 'audience_average': ['mean', 'std'], 'critic_average':['mean', 'std']}).reset_index()
    averages.columns = averages.columns.get_level_values(1)
    averages.columns.values[0] = 'cast_member'
    averages.columns.values[2] = 'audience_average'
    averages.columns.values[3] = 'audience_std'
    averages.columns.values[4] = 'critic_average'
    averages.columns.values[5] = 'critic_std'
    averages = averages.sort_values(
        by=['count', 'critic_average', 'audience_average'], ascending=False)


    ''' ------------------------------------------------------------ '''
    # Count number of movies directed and average ratings of directors 

    data_dir = joined[['director', 'audience_average', 'critic_average']]
    data_dir = data_dir.dropna()

    exploded_dir = data_dir.director.apply(
        pd.Series).stack().reset_index(
        level=1, drop=True).to_frame('director')
    df2 = exploded_dir.join(data_dir['audience_average'])
    df2 = df2.join(data_dir['critic_average'])

    averages_dir = df2.groupby('director').agg(
        {'director': ['count'], 'audience_average': ['mean', 'std'] , 'critic_average': ['mean', 'std']}).reset_index()
    
    averages_dir.columns = averages_dir.columns.get_level_values(1)
    averages_dir.columns.values[0] = 'director'
    averages_dir.columns.values[2] = 'audience_average'
    averages_dir.columns.values[3] = 'audience_std'
    averages_dir.columns.values[4] = 'critic_average'
    averages_dir.columns.values[5] = 'critic_std'

    averages_dir = averages_dir.sort_values(
        by=['count', 'critic_average' ,'audience_average'], ascending=False)
    averages_dir = averages_dir.head(10)
    averages = averages.head(10)
    
    x = np.linspace(1,10,10)

    plt.style.use('seaborn-deep')
    plt.title('Average Ratings of Top 10 Directors based on Number of Works')
    plt.plot(x, averages_dir['audience_average'], 'r')
    plt.plot(x, averages_dir['critic_average'], 'b')
    plt.legend(['audience rating', 'critic rating'])
    plt.savefig('top_directors')
    # plt.show()


if __name__ == '__main__':
    main()
