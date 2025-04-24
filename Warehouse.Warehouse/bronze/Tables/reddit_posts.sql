CREATE TABLE [bronze].[reddit_posts] (

	[id] varchar(10) NOT NULL, 
	[titulo] varchar(1000) NULL, 
	[autor] varchar(255) NULL, 
	[upvotes] bigint NULL, 
	[comentarios] int NULL, 
	[link] varchar(2000) NULL, 
	[subreddit] varchar(255) NULL, 
	[data_postagem] datetime2(6) NULL, 
	[data_coleta] datetime2(6) NULL, 
	[data_processamento] datetime2(6) NULL
);