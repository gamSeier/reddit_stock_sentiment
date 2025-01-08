# Reddit Stock Sentiment Model

The model monitors all comments for a subset of finance-focused Reddit threads in real time via a Apache Kafka producer. The Kafka consumer takes all the comments for the subreddits and identifies mentioned S&P 500 tickers, and calculates a sentiment score via the vaderSentiment Python package. The data is then sent through to a MongoDB database, which is then connected to a Tableau dashboard for real time monitoring.
