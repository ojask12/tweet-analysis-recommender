import snscrape.modules.twitter as sntwitter
import pandas as pd
from datetime import datetime, timedelta

from sklearn.feature_extraction.text import TfidfVectorizer
from scipy import spatial
import spacy

import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords


def get_top_similar_tweets(t_id, user_name, user_topic):
    """
    input: tweet_id, user name and user query
    output: top 10 most similar + popular tweets
    """
    
    def utility(q, limit=100):
        tweets = []
        for tweet in sntwitter.TwitterSearchScraper(q).get_items():

            if len(tweets) == limit:
                break

            else:
                #tweet details
                try:
                    url = tweet.url
                except AttributeError:
                    print('AttributeError @url')
                    url = None

                try:
                    tweetDate = tweet.date
                except AttributeError:
                    print('AttributeError @tweetDate')
                    tweetDate = None 

                try:
                    content = tweet.content
                except AttributeError:
                    print('AttributeError @content')
                    content = None 

                try:
                    tweetId = str(tweet.id)
                except AttributeError:
                    print('AttributeError @id')
                    tweetId = None 

                try:
                    replyCount = int(tweet.replyCount)
                except AttributeError:
                    print('AttributeError @replyCount')
                    replyCount = None 

                try:
                    retweetCount = int(tweet.retweetCount)
                except AttributeError:
                    print('AttributeError @retweetCount')
                    retweetCount = None 

                try:
                    likeCount = int(tweet.likeCount)
                except AttributeError:
                    print('AttributeError @likeCount')
                    likeCount = None 

                try:
                    quoteCount = int(tweet.quoteCount)
                except AttributeError:
                    print('AttributeError @quoteCount')
                    quoteCount = None 

                try:
                    conversationId = str(tweet.conversationId)
                except AttributeError:
                    print('AttributeError @conversationId')
                    conversationId = None 

                try:
                    language = tweet.lang
                except AttributeError:
                    print('AttributeError @language')
                    language = None 

                try:
                    outlinks = tweet.outlinks
                except AttributeError:
                    print('AttributeError @outlinks')
                    outlinks = None 

                try:
                    media = tweet.media
                except AttributeError:
                    print('AttributeError @media')
                    media = None 

                try:
                    retweetedTweet = str(tweet.retweetedTweet)
                except AttributeError:
                    print('AttributeError @retweetedTweet')
                    retweetedTweet = None 

                try:
                    quotedTweet = str(tweet.quotedTweet)
                except AttributeError:
                    print('AttributeError @quotedTweet')
                    quotedTweet = None 

                try: 
                    inReplyToTweetId = str(tweet.inReplyToTweetId)
                except AttributeError:
                    print('AttributeError @inReplyToTweetId')
                    inReplyToTweetId = None 

                try:
                    mentionedUsers = tweet.mentionedUsers
                except AttributeError:
                    print('AttributeError @mentionedUsers')
                    mentionedUsers = None 

                try:
                    hashtags = tweet.hashtags
                except AttributeError:
                    print('AttributeError @hashtags')
                    hashtags = None 

                try:
                    place = tweet.place
                except AttributeError:
                    print('AttributeError @place')
                    place = None 

                #user details

                try:
                    userId = str(tweet.user.id)
                except AttributeError:
                    print('AttributeError @userid')
                    userId = None 

                try: 
                    username = tweet.user.username
                except AttributeError:
                    print('AttributeError @username')
                    username = None 

                try:
                    displayname = tweet.user.displayname
                except AttributeError:
                    print('AttributeError @displayname')
                    displayname = None 

                try:
                    description = tweet.user.description
                except AttributeError:
                    print('AttributeError @userDescription')
                    description = None 

                try:
                    descriptionUrls = tweet.user.descriptionUrls
                except AttributeError:
                    print('AttributeError @descriptionUrls')
                    descriptionUrls = None 

                try: 
                    profileCreated = tweet.user.created
                except AttributeError:
                    print('AttributeError @profileCreated')
                    profileCreated = None 

                try: 
                    followersCount = int(tweet.user.followersCount)
                except AttributeError:
                    print('AttributeError @followersCount')
                    followersCount = None 

                try:
                    friendsCount = int(tweet.user.friendsCount)
                except AttributeError:
                    print('AttributeError @friendsCount')
                    friendsCount = None 

                try:
                    statusesCount = int(tweet.user.statusesCount)
                except AttributeError:
                    print('AttributeError @statusesCount')
                    statusesCount = None 

                try:
                    favouritesCount = int(tweet.user.favouritesCount)
                except AttributeError:
                    print('AttributeError @favoritesCount')
                    favouritesCount = None 

                try:
                    listedCount = int(tweet.user.listedCount)
                except AttributeError:
                    print('AttributeError @listedCount')
                    listedCount = None 

                try:
                    mediaCount = int(tweet.user.mediaCount)
                except AttributeError:
                    print('AttributeError @mediaCount')
                    mediaCount = None 

                try:
                    location = tweet.user.location
                except AttributeError:
                    print('AttributeError @location')
                    location = None 

                try:  
                    linkUrl = tweet.user.linkUrl
                except AttributeError:
                    print('AttributeError @linkUrl')
                    linkUrl = None 


                tweets.append([url,
                            tweetDate, # unix epoch timestamp in milliseconds
                            content,
                            tweetId,
                            replyCount,
                            retweetCount,
                            likeCount,
                            quoteCount,
                            conversationId,
                            language,
                            retweetedTweet,
                            quotedTweet,
                            inReplyToTweetId,
                            userId,
                            username,
                            displayname,
                            description,
                            followersCount,
                            friendsCount,
                            statusesCount,
                            favouritesCount,
                            listedCount,
                            mediaCount])

        tweets_df = pd.DataFrame(tweets, columns=['url',
                            'tweet_date',
                            'content',
                            'tweet_id',
                            'reply_count',
                            'retweet_count',
                            'like_count',
                            'quote_count',
                            'conversation_id',
                            'language',
                            'retweeted_tweet_id',
                            'quoted_tweet_id',
                            'inreply_to_tweet_id',
                            'user_id',
                            'username',
                            'displayname',
                            'description',
                            'followers_count',
                            'friends_count',
                            'statuses_count',
                            'favourites_count',
                            'listed_count',
                            'media_count'])


        return tweets_df

    def get_topic_specific_tweets(topic):
        query = f'{topic} lang:en'
        return utility(query, 1000)

    def get_user_history(username, upper_limit=180):
        query = f'from:{username} include:nativeretweets lang:en since:{(datetime.now() - timedelta(days=upper_limit)).strftime("%Y-%m-%d")}'
        return utility(query)
    
    
    user_df = get_user_history(user_name)
    
#     word_list = {}
#     for i in range(len(doc)):
#         token = doc[i]
#         if token.pos_ in pos_reqd:   
#             if token.text.isalpha():
#                 word_list[token.text] = token.pos_

#     user_tweet_query = '"' + '" OR "'.join(word_list.keys()) + '"'

    
    min_replies = user_df[user_df.tweet_id == t_id]['reply_count']
    min_faves = user_df[user_df.tweet_id == t_id]['like_count']
    min_retweets = user_df[user_df.tweet_id == t_id]['retweet_count']

    user_tweet_query = '("' + '" OR "'.join([word.strip() for word in user_input.split(",")]) + '")' +  f' min_replies:{20} min_faves:{671} min_retweets:{95} -from:{user_name} lang:en'
    
    # user_tweet_query += f' -from:{user_name} lang:en'
    
    topic_tweets = get_topic_specific_tweets(user_tweet_query)
    
    merged_df = pd.concat([topic_tweets], axis=0)
    merged_df = merged_df.reset_index(drop=True)
    merged_df = merged_df.rename(columns={'content': 'tweet_content'})    
    
    stop_words = []

    f = open("StopWords_Generic.txt", "r")
    stop_text = f.read()
    stop_words_upper = stop_text.split("\n")
    for word in stop_words_upper:
        stop_words.append(word.lower())

    for word in stopwords.words('english'):
        if word not in stop_words:
            stop_words.append(word)
            
    
    vectorizer = TfidfVectorizer(stop_words=stop_words)
    X = vectorizer.fit_transform(merged_df['tweet_content'].tolist())

    vectorized_df = pd.DataFrame(X.toarray(), columns=list(vectorizer.get_feature_names_out()))

    final_df = pd.concat([merged_df, vectorized_df], axis=1)

    sim_scores = [0] * len(final_df)
    
    content = user_df[user_df.tweet_id == t_id]['content']
    # print(content.iloc[0])
    pred = vectorizer.transform([content.iloc[0]]).toarray()[0]
    
    for j in range(len(final_df)):
        row = list(final_df.iloc[j])[-len(pred):]
        sim_scores[j] += (1 - spatial.distance.cosine(pred, row))
    
    final_df['scores'] = pd.Series(sim_scores)
    final_df = final_df.sort_values('scores', ascending=False)
    final_df = final_df.reset_index(drop=True)
    
    top = 100
    
    most_similar_tweets_df = final_df.iloc[:top,:]
    
    w1 = 0.5
    w2 = 0.3
    w3 = 0.2
    
    most_similar_tweets_df.loc[:,'popularity'] = ((w1*most_similar_tweets_df.loc[:,'reply_count']) + (w2*most_similar_tweets_df.loc[:,'retweet_count']) + (w3*most_similar_tweets_df.loc[:,'like_count'])) / (most_similar_tweets_df.loc[:,'followers_count'])
    most_similar_tweets_df = most_similar_tweets_df.sort_values('popularity', ascending=False)
    most_similar_tweets_df = most_similar_tweets_df.reset_index(drop=True)
    
    return most_similar_tweets_df.iloc[:10,:][['tweet_id', 'tweet_date', 'tweet_content', 'like_count', 'reply_count', 'retweet_count', 'quote_count', 'popularity']]
