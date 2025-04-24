CREATE TABLE [bronze].[youtube_categories] (

	[kind] varchar(8000) NULL, 
	[etag] varchar(8000) NULL, 
	[items.kind] varchar(8000) NULL, 
	[items.etag] varchar(8000) NULL, 
	[items.id] varchar(8000) NULL, 
	[items.snippet.title] varchar(8000) NULL, 
	[items.snippet.assignable] bit NULL, 
	[items.snippet.channelId] varchar(8000) NULL
);