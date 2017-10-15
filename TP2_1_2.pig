file = LOAD '/mnt/c/Users/zhouj/pig_Project/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);
-- Filter the first line of csv (header	)
withOutHeader = FILTER file BY $0>1;
-- Show	the	total number of reviews for	each movie
groupByMovieId = GROUP withOutHeader BY movieId;
getIdReviersCount = FOREACH groupByMovieId GENERATE group AS movieId, COUNT(withOutHeader.movieId) AS count;
orderResult = ORDER getIdReviersCount BY $1;
STORE orderResult INTO 'lab2_1_2_output' USING PigStorage(',');