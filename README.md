## README

### Dependencies
Make sure to have Pandas, NumPy, and PySpark installed before running any of these

To get started, you will need these datasets from the `movies` folder in the cluster:
- `wikidata-readable` that is generated from `build_useful_movies.py`
- `label_map`
- `rotten-tomatoes.json.gz`

## Important Files
- `build_useful_movies.py` 
- `correlations.py`
- `analyze.py`
- `profitable_time.py`

### `build_useful_movies.py`
run command: `spark-submit build_useful_movies.py wikidata-movies label_map output`

Cleans the data output from `build_wikidata_movies.py` and maps all wikidata ids to its respective labels in `label_map`

outputs: `movies-readable.json.gz`

### `correlations.py`
run command: `python3 correlations.py`

Loads `movies-readable.json.gz` and `rotten-tomatoes.json.gz` into the program, and outputs the correlations of specific columns in the dataset into the terminal using a bunch of `print` statements. Summary of the results can be found in the **Project Summary**

### `analyze.py`
run command: `python3 analyze.py`

Loads `movies-readable.json.gz` and `rotten-tomatoes.json.gz` into the program again, and takes a deeper look into the data. Mainly focused on `cast_member`, `director`, and `made_profit`. Outputs four graphs that gives insight on movie profit and ratings.

outputs: 
- top_directors.png
- top_actors.png
- directors_sd.png
- actors_sd.png

### `profitable_time.py`
run command: `python3 profitable_time.py`

Loads `movies-readable.json.gz` and `rotten-tomatoes.json.gz`, and groups the movies/works that made profit by publication month.
Outputs a histogram on what publication month had the most movies that made profit.

output: month-profit-count.png

**Note that both `movies-readable.json.gz` and `rotten-tomatoes.json.gz` should be in the same folder as the Python programs above**


