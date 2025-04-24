CREATE TABLE [bronze].[reddit_comunidades] (

	[post_id] varchar(10) NOT NULL, 
	[subreddit] varchar(255) NOT NULL, 
	[data_coleta] datetime2(6) NULL, 
	[data_processamento] datetime2(6) NULL
);