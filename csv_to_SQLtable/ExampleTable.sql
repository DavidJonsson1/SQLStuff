USE tempdb
GO

IF OBJECT_ID(N'dbo.person_info', N'U') IS NULL
BEGIN
	CREATE TABLE [dbo].[person_info](
		[Name] [nvarchar](100) NULL,
		[ID] [nvarchar](100) NOT NULL
		)

	ALTER TABLE [dbo].[person_info] ADD  CONSTRAINT [PK_person_info] PRIMARY KEY CLUSTERED 
	(
		[ID] ASC
	)
END;