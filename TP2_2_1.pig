ratings = LOAD '/mnt/c/Users/zhouj/pig_Project/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);
movies = LOAD '/mnt/c/Users/zhouj/pig_Project/ml-20m/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray, genres:chararray);
-- Filter the first line of csv (header	)
ratingsWithOutHeader = FILTER ratings BY $0>0;
moviesWithOutHeader = FILTER movies BY $0>0;
-- Deal with movies:
-- newMovies = FOREACH moviesWithOutHeader GENERATE movieId,title,TOKENIZE(TRIM(genres), '|');
-- FLATTEN() open a tuple or bag
-- A = FOREACH newMovies { GENERATE movieId,FLATTEN(genres) AS g;};
newMovies = FILTER moviesWithOutHeader BY genres MATCHES '.*Documentary.*';

moviesRating = COGROUP newMovies BY movieId, ratingsWithOutHeader BY movieId;
filterMovies = FILTER moviesRating BY SIZE(newMovies) != 0;

avgRating = FOREACH filterMovies GENERATE newMovies,AVG(ratingsWithOutHeader.rating) as avgrating;

/* Problem:
* If i order avgRating according to the avgrating
* Every thing goes well
* But in the result, I cannot find the data of avgrating
* But if i just output the result of avgRating, i could get the data of avgrating
*/
-- orderResult = ORDER avgRating BY avgrating;
-- DESCRIBE orderResult;
-- STORE orderResult INTO 'lab2_2_1_output' USING PigStorage(',');

STORE avgRating INTO 'lab2_2_1_output' USING PigStorage(',');