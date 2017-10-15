# Apache_Pig

This is a project about how to use apache pig

In this case, I decided use 4 pig file to realize 4 different requirement:
1	Show all users which have more than 100	reviews
2	Show the total number of reviews for each movie
3	The average rating for ’Documentary’ movies
4	For	each ’Action’ movie, the total number of tags that have been added


Preparation:

Firsr, we need to import the source file. Usually, we should import some files from HDFS of Hadoop, but in this case, we don't need to focus on Hadoop's part, so we can direclt import from local path.
file = LOAD '<Path of file>' USING PigStorage(',') AS (var:class);

Attention, var is the name of one col, class is the type of this col, we have Int, Long, CharArray etc.. in pig.

If we import a file who have header, and we dont want to read it, we can filter it by:
filterHeader = FILTER file BY $0>0;


1.(TP2_1_1.pig)

Because we want to get the user who have more than 100 reviews, we should group the datas by userid:
groupByUserId = GROUP withOutHeader BY userId;

And the we can use function COUNT(exp) to get the total number of exp.

getIdReviersCount = FOREACH groupByUserId GENERATE group AS userId, COUNT(withOutHeader.userId) AS count;

For now we got the total number of reviews of each user, and we can filter the user who have more than 100 reviews:

filterCount = FILTER getIdReviersCount BY $1>100;
* BTW, $1 means the 2ed col in the bag.

And then I make a order to get te rank of reviews
orderResult = ORDER filterCount BY $1 DESC;

Resulat of top 10:
userId,number of reviews
118205,9254
8405,7515
82418,5646
121535,5520
125794,5491
74142,5447
34576,5356
131904,5330
83090,5169
59477,4988

2.(TP2_1_2.pig)

First, group by the movieid:
groupByMovieId = GROUP withOutHeader BY movieId;

And then, according to the movieid to count the number:
getIdReviersCount = FOREACH groupByMovieId GENERATE group AS movieId, COUNT(withOutHeader.movieId) AS count;

Order the data:
orderResult = ORDER getIdReviersCount BY $1;

Result:
movieId,count of reviews
96842,1
96851,1
101754,1
96903,1
101730,1
101726,1
96907,1
128669,1
96919,1
96921,1

3.(TP2_2_1.pig)

Get the row who have the sub-string: Documentary in genres of movies, And used RegEX .*Documentary.* (This is a way to use RegEx in pig) to find it:
newMovies = FILTER moviesWithOutHeader BY genres MATCHES '.*Documentary.*';

group and union two bags by movieId:
moviesRating = COGROUP newMovies BY movieId, ratingsWithOutHeader BY movieId;

With above commend, we will get some row who's $1 is null like:{}, so we filter it:
filterMovies = FILTER moviesRating BY SIZE(newMovies) != 0;

And then we can calcul the avg of rating:
avgRating = FOREACH filterMovies GENERATE newMovies,AVG(ratingsWithOutHeader.rating) as avgrating;

Result:
row of movies.csv                                         avg of rating
{(37,Across the Sea of Time (1995),Documentary|IMAX)},3.0176470588235293
{(77,Nico Icon (1995),Documentary)},3.429864253393665
{(99,Heidi Fleiss: Hollywood Madam (1995),Documentary)},3.105371900826446
{(108,Catwalk (1996),Documentary)},3.092
{(116,Anne Frank Remembered (1995),Documentary)},3.9373024236037937
{(128,Jupiter's Wife (1994),Documentary)},3.5
{(134,Sonic Outlaws (1995),Documentary)},3.6527777777777777
{(136,From the Journals of Jean Seberg (1995),Documentary)},3.549019607843137
{(137,Man of the Year (1995),Documentary)},3.2853982300884956
{(162,Crumb (1994),Documentary)},4.009924937447873

4.(TP2_2_2.pig)

The same commend as the question3, but, we need to relation another file, and the change the avg to count:
countTags = FOREACH filterMovies GENERATE newMovies,COUNT(tagsWithOutHeader.tag);

Result:
row of movies.csv                      number of tags
{(6,Heat (1995),Action|Crime|Thriller)},259
{(9,Sudden Death (1995),Action)},9
{(10,GoldenEye (1995),Action|Adventure|Thriller)},99
{(15,Cutthroat Island (1995),Action|Adventure|Romance)},28
{(20,Money Train (1995),Action|Comedy|Crime|Drama|Thriller)},6
{(23,Assassins (1995),Action|Crime|Thriller)},10
{(42,Dead Presidents (1995),Action|Crime|Drama)},3
{(44,Mortal Kombat (1995),Action|Adventure|Fantasy)},51
{(51,Guardian Angel (1994),Action|Drama|Thriller)},0
{(66,Lawnmower Man 2: Beyond Cyberspace (1996),Action|Sci-Fi|Thriller)},5
