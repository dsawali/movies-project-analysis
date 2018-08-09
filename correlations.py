import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
import sys
from scipy import stats



def main():
    wiki = pd.read_json('movies-readable.json.gz', lines=True)
    tomato = pd.read_json('rotten-tomatoes.json.gz', lines=True)
    tomato = tomato.drop(columns='rotten_tomatoes_id')

    ''' ------------------------------------------------------------------------------ '''
    audience = tomato['audience_average'].dropna()
    director_profit = wiki[['director', 'made_profit']].dropna()
    ''' ------------------------------------------------------------------------------ '''
    
    '''join wiki movies and rotten tomatoes'''
    joined = wiki.merge(tomato, on='imdb_id', how='inner')
    joined['publication_date'] = joined['publication_date'].astype(str).str[:4]
    joined['publication_date'] = joined['publication_date'].astype(int)
    joined = joined[joined['publication_date'] > 1999]

    profit = joined[['director','cast_member','critic_average', 'audience_average', 'audience_percent', 'critic_percent', 'made_profit']]
    profit = profit.dropna()
    profitable = profit.dropna(subset=['made_profit'])
    
    score = joined[['director', 'cast_member', 'genre', 'critic_average', 'audience_average', 'made_profit']]
    score = score[score['genre'] != 'documentary film']


    
    e = profitable.cast_member.apply(pd.Series).stack().reset_index(level=1, drop=True).to_frame('cast_member')
    f = e.join(profitable['made_profit'])
    f['cast_member'] = f['cast_member'].astype('category').cat.codes
    f = f.dropna()

    g = profitable.director.apply(pd.Series).stack().reset_index(level=1, drop=True).to_frame('director')
    h = g.join(profitable['made_profit'])
    h['director'] = h['director'].astype('category').cat.codes
    h = h.dropna()    

    print('Correlations Between: ')
    

    print('\tcast member and profit: ', f['cast_member'].corr(f['made_profit']))
    print('\tdirector and profit: ', h['director'].corr(h['made_profit']))
    print('\taudience average and profit: ', profit['audience_average'].corr(profit['made_profit']))
    print('\tcritic average and profit: ', profit['critic_average'].corr(profit['made_profit']))
    print('\taudience percentage and profit: ', profit['audience_percent'].corr(profit['made_profit']))
    print('\tcritic percentage and profit: ', profit['critic_percent'].corr(profit['made_profit']))
    print('\taudience average rating and critic average rating: ', tomato['audience_average'].corr(tomato['critic_average']))
    print('\taudience percent and critic percent: ', tomato['audience_percent'].corr(tomato['critic_percent']))
    

   
if __name__ == '__main__':
    main()
