movies = LOAD '/mnt/c/Users/zhouj/pig_Project/ml-20m/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray, genres:chararray);
tags = LOAD '/mnt/c/Users/zhouj/pig_Project/ml-20m/tags.csv' USING PigStorage(',') AS (userId:int, movieId:int, tag:chararray, timestamp:long);
-- Filter the first line of csv (header	)
moviesWithOutHeader = FILTER movies BY $0>0;
tagsWithOutHeader = FILTER tags BY $0>0;

newMovies = FILTER moviesWithOutHeader BY genres MATCHES '.*Action.*';

moviesTags = COGROUP newMovies BY movieId, tagsWithOutHeader BY movieId;

filterMovies = FILTER moviesTags BY SIZE(newMovies) != 0;

countTags = FOREACH filterMovies GENERATE newMovies,COUNT(tagsWithOutHeader.tag);

STORE countTags INTO 'lab2_2_2_output' USING PigStorage(',');