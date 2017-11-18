# Twitter analysis with map reduce
Analysing a dataset of tweets collected during the 2016 Rio Olympic Games. 
Creating several Map/Reduce programs to perform multiple types of computation. 

The dataset contains a large collection of Twitter messages captured during the Rio 2016 Olympics period. The messages were collected by connecting to Twitter Streaming API, and filtering only messages directly related to the Olympic Games (by requesting they include a related hashtag such as #Rio2016 or #rioolympics ).

Example line: epoch_time;tweetId;tweet(including #hashtags);device

PART A. MESSAGE LENGTH ANALYSIS
Creating a histogram plot that depicts the distribution of tweet sizes (measured in number of characters) among the Twitter dataset. 

PART B. TIME ANALYSIS
1.Creating a bar plot showing the number of Tweets that were posted each hour of the event.
2.Computing the top 10 hashtags that were emitted during the most popular hour.

PART C. SUPPORT ANALYSIS
1.Top 30 athletes in number of mentions across the dataset
2.Top 20 sports according to the mentions of olympic athletes captured
