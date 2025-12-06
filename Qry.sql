CREATE Or ALTER PROCEDURE [dbo].[sp_CreateTableFromDefinition]
(
    @TableName NVARCHAR(200),
    @ColumnsDefinition NVARCHAR(MAX)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    SET @SQL = 'CREATE TABLE [' + @TableName + '] (' + @ColumnsDefinition + ')';

    EXEC (@SQL);
END
GO


CREATE Or ALTER PROCEDURE [dbo].[Import_Dynamic_JSON_Columns] (
    @TableName SYSNAME,
    @JsonData NVARCHAR(MAX)
)
AS
BEGIN
    SET NOCOUNT ON;

    -- Variable declarations
    DECLARE @ColumnList NVARCHAR(MAX); -- For the INSERT statement column list
    DECLARE @JsonSchema NVARCHAR(MAX);  -- For the OPENJSON WITH clause schema
    DECLARE @sqlCommand NVARCHAR(MAX);
	DECLARE @sqlError NVARCHAR(MAX);
	DECLARE @cols NVARCHAR(MAX)

    -- 1. Validate the table name
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @TableName)
    BEGIN
        RAISERROR('Target table "%s" does not exist.', 16, 1, @TableName);
        RETURN;
    END

	----------------------------------------
    -- 2. Create Error Log Table (Corrected Logic)
    -- Only create it if it doesn't exist.
    ----------------------------------------
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CsvImportErrors')
    BEGIN
        CREATE TABLE [dbo].[CsvImportErrors](
            [ErrId] [int] IDENTITY(1,1) NOT NULL,
            [RowNumber] [int] NULL,
            [ErrorMessage] [nvarchar](500) NULL,
            [LogDate] [datetime] NULL, -- Corrected column name
            PRIMARY KEY CLUSTERED 
            (
                [ErrId] ASC
            )
        ) ON [PRIMARY];
    END
    -- If the table exists, we assume we want to truncate it, not drop/recreate the structure.
    -- If you want to clear the errors from a previous run:
    TRUNCATE TABLE dbo.CsvImportErrors;
	EXEC dbo.sp_TruncateTable @TableName = @TableName;

    -- 3. Dynamically build the Column List and OPENJSON Schema
    -- ... (Skipping the metadata query as it's correct)
    SELECT
        @ColumnList = STRING_AGG(QUOTENAME(c.name), ', '),
        @JsonSchema = STRING_AGG(
            QUOTENAME(c.name) + N' ' +
            t.name +
            CASE
                WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary') AND c.max_length <> -1 THEN '(' + CAST(c.max_length AS NVARCHAR(10)) + ')'
                WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar') AND c.max_length = -1 THEN '(MAX)'
                WHEN t.name IN ('decimal', 'numeric') THEN '(' + CAST(c.precision AS NVARCHAR(10)) + ', ' + CAST(c.scale AS NVARCHAR(10)) + ')'
                ELSE ''
            END,
            ', '
        )
    FROM
        sys.columns c
    INNER JOIN
        sys.types t ON c.user_type_id = t.user_type_id
    WHERE
        c.object_id = OBJECT_ID(@TableName)
        AND c.is_identity = 0;
    
    
    -- Check for metadata success
    IF @ColumnList IS NULL OR @JsonSchema IS NULL
    BEGIN
        RAISERROR('Could not retrieve column metadata for table "%s".', 16, 1, @TableName);
        RETURN;
    END

    ----------------------------------------
    -- 4. Build the Dynamic INSERT statement (Wrap in TRY/CATCH for error logging)
    ----------------------------------------
    SET @sqlCommand = N'
    BEGIN TRY
        INSERT INTO ' + QUOTENAME(@TableName) + N' (' + @ColumnList + N')
        SELECT ' + @ColumnList + N'
        FROM
            OPENJSON(@inputJson)
            WITH (' + @JsonSchema + N') AS JSON_Data;
    END TRY
    BEGIN CATCH
        -- Insert error details into the log table
        INSERT INTO dbo.CsvImportErrors (ErrorMessage, LogDate) 
        VALUES (
            ''Dynamic INSERT failed for table '' + @TableName + '': '' + ERROR_MESSAGE(), 
            GETDATE()
        );
        THROW; -- Re-throw the error to notify the caller
    END CATCH
    ';

    -- 5. Execute the Dynamic SQL
    -- We pass @TableName into sp_executesql for use inside the CATCH block
    EXEC sp_executesql
        @sqlCommand,
        N'@inputJson NVARCHAR(MAX), @TableName SYSNAME',
        @inputJson = @JsonData,
        @TableName = @TableName; -- Pass the table name for use in the error log

END
GO


CREATE PROCEDURE dbo.sp_TruncateTable
    @TableName NVARCHAR(128)
AS
BEGIN
    -- 1. Check if the table name is valid (optional but recommended)
    IF OBJECT_ID(@TableName, 'U') IS NOT NULL
    BEGIN
        -- 2. Construct the TRUNCATE statement
        -- Use QUOTENAME to safely handle table names with spaces or special characters
        DECLARE @SqlStatement NVARCHAR(MAX);
        SET @SqlStatement = N'TRUNCATE TABLE ' + QUOTENAME(@TableName) + ';';

        -- 3. Execute the Dynamic SQL
        EXEC sp_executesql @SqlStatement;

        -- Optional: Provide feedback
        SELECT 'Table ' + @TableName + ' has been truncated.' AS Result;
    END
    ELSE
    BEGIN
        -- Optional: Handle case where table doesn't exist
        RAISERROR('Table name %s does not exist or is invalid.', 16, 1, @TableName);
    END
END
GO


