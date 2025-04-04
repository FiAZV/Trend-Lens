CREATE TABLE [bronze].[youtube_videos] (

	[kind] varchar(8000) NULL, 
	[etag] varchar(8000) NULL, 
	[items.kind] varchar(8000) NULL, 
	[items.etag] varchar(8000) NULL, 
	[items.id] varchar(8000) NULL, 
	[items.snippet.publishedAt] varchar(8000) NULL, 
	[items.snippet.channelId] varchar(8000) NULL, 
	[items.snippet.title] varchar(8000) NULL, 
	[items.snippet.description] varchar(8000) NULL, 
	[items.snippet.thumbnails.default.url] varchar(8000) NULL, 
	[items.snippet.thumbnails.default.width] bigint NULL, 
	[items.snippet.thumbnails.default.height] bigint NULL, 
	[items.snippet.thumbnails.medium.url] varchar(8000) NULL, 
	[items.snippet.thumbnails.medium.width] bigint NULL, 
	[items.snippet.thumbnails.medium.height] bigint NULL, 
	[items.snippet.thumbnails.high.url] varchar(8000) NULL, 
	[items.snippet.thumbnails.high.width] bigint NULL, 
	[items.snippet.thumbnails.high.height] bigint NULL, 
	[items.snippet.thumbnails.standard.url] varchar(8000) NULL, 
	[items.snippet.thumbnails.standard.width] bigint NULL, 
	[items.snippet.thumbnails.standard.height] bigint NULL, 
	[items.snippet.thumbnails.maxres.url] varchar(8000) NULL, 
	[items.snippet.thumbnails.maxres.width] bigint NULL, 
	[items.snippet.thumbnails.maxres.height] bigint NULL, 
	[items.snippet.channelTitle] varchar(8000) NULL, 
	[items.snippet.categoryId] varchar(8000) NULL, 
	[items.snippet.liveBroadcastContent] varchar(8000) NULL, 
	[items.snippet.localized.title] varchar(8000) NULL, 
	[items.snippet.localized.description] varchar(8000) NULL, 
	[items.contentDetails.duration] varchar(8000) NULL, 
	[items.contentDetails.dimension] varchar(8000) NULL, 
	[items.contentDetails.definition] varchar(8000) NULL, 
	[items.contentDetails.caption] varchar(8000) NULL, 
	[items.contentDetails.licensedContent] bit NULL, 
	[items.contentDetails.regionRestriction.blocked] varchar(8000) NULL, 
	[items.contentDetails.projection] varchar(8000) NULL, 
	[items.statistics.viewCount] varchar(8000) NULL, 
	[items.statistics.likeCount] varchar(8000) NULL, 
	[items.statistics.favoriteCount] varchar(8000) NULL, 
	[items.statistics.commentCount] varchar(8000) NULL, 
	[nextPageToken] varchar(8000) NULL, 
	[pageInfo.totalResults] bigint NULL, 
	[pageInfo.resultsPerPage] bigint NULL, 
	[items.snippet.tags] varchar(8000) NULL, 
	[items.snippet.defaultLanguage] varchar(8000) NULL, 
	[items.snippet.defaultAudioLanguage] varchar(8000) NULL, 
	[items.contentDetails.regionRestriction.allowed] varchar(8000) NULL
);

