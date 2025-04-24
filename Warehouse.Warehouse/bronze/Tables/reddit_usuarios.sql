CREATE TABLE [bronze].[reddit_usuarios] (

	[nome] varchar(255) NOT NULL, 
	[karma_total] bigint NULL, 
	[karma_post] bigint NULL, 
	[karma_comentario] bigint NULL, 
	[data_criacao] datetime2(6) NULL, 
	[premium] bit NULL, 
	[data_coleta] datetime2(6) NULL, 
	[data_processamento] datetime2(6) NULL
);