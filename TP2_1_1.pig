file = LOAD '/mnt/c/Users/zhouj/pig_Project/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);
-- Filter the first line of csv (header	)
withOutHeader = FILTER file BY $0>0;
-- Show all users which have more than 100	reviews
groupByUserId = GROUP withOutHeader BY userId;
getIdReviersCount = FOREACH groupByUserId GENERATE group AS userId, COUNT(withOutHeader.userId) AS count;
filterCount = FILTER getIdReviersCount BY $1>100;
orderResult = ORDER filterCount BY $1 DESC;
STORE orderResult INTO 'lab2_1_1_output' USING PigStorage(',');
