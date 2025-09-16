/****************************************************************************************************
 Nombre del procedimiento : objectMigrationTool
 Autor                    : Cristhian Arroyo
 Fecha de creación        : 2025-08-08
 Descripción              : Este procedimiento tiene como propósito migrar todos los objetos de una base 
                            de datos a archivos físicos en disco, con el fin de facilitar el versionado 
                            y control de cambios de los objetos de SQL Server.
 
 Parámetros:
    @targetPath       NVARCHAR(MAX) 
                      Ruta destino donde se guardarán los archivos generados.
                      Valor por defecto: 'D:\temp\'

    @perTypeSubFolder BIT
                      Indica si se crearán subcarpetas por tipo de objeto (tablas, vistas, SPs, etc.).
                      1 = Sí (crea subcarpetas)
                      0 = No (todos los objetos en la misma carpeta)
                      Valor por defecto: 1

    @overWriteFiles   BIT
                      Controla si se sobrescribirán los archivos en caso de que ya existan en el directorio.
                      1 = Sí (sobrescribe archivos existentes)
                      0 = No (mantiene los archivos existentes)
                      Valor por defecto: 1

    @bcpExe           VARCHAR(500)
                      Ruta completa al ejecutable de la utilidad BCP (Bulk Copy Program), que se usará para exportar datos.
                      Valor por defecto: 'C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe'
****************************************************************************************************/

DECLARE @targetPath NVARCHAR(MAX) = 'D:\temp\',
		@perTypeSubFolder BIT = 1,
		@overWriteFiles BIT = 0,
		@bcpExe VARCHAR(500) = 'C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe';

DECLARE @dbName SYSNAME,
		@SQL NVARCHAR(MAX),
		@dbTargetPath VARCHAR(2000),
		@sep VARCHAR(1),
		@objTypes VARCHAR(150),
		@objTypesPath NVARCHAR(2000),
		@base NVARCHAR(MAX),
		@advWasEnabled BIT,
		@xpWasEnabled  BIT,
		@rc INTEGER,
		@cmd VARCHAR(8000),
		@lastId INTEGER = 0,
		@bcpTargetPath VARCHAR(MAX),
		@query VARCHAR(MAX),
		@serverName VARCHAR(255) = @@SERVERNAME,
		@exists BIT,
		@dir NVARCHAR(2000);

DECLARE @out TABLE(line NVARCHAR(4000));


DECLARE @routeTable TABLE(
		routesId INTEGER IDENTITY(1,1) PRIMARY KEY,
		databaseName VARCHAR(150),
		objectType VARCHAR(150)
		);

IF OBJECT_ID('master..objectList') IS NULL
	BEGIN
		CREATE TABLE master..objectList (
		objectTypeId INTEGER IDENTITY(1,1) PRIMARY KEY,
		schemaName VARCHAR(150), 
		objectName VARCHAR(150),
		typeFolder VARCHAR(100), 
		definition VARCHAR(MAX),
		TargetPath VARCHAR(MAX),
		processed BIT DEFAULT 0
		);
	END
ELSE
	BEGIN
		TRUNCATE TABLE master..objectList
	END;
	
BEGIN
	SET NOCOUNT ON;

	IF @targetPath IS NULL
		BEGIN
			RAISERROR('There is no target path. Please set a Target Path.',16, 1);
			RETURN;
		END

	IF NOT EXISTS (SELECT * FROM sys.dm_os_file_exists(@targetPath) WHERE file_is_a_directory = 1 AND parent_directory_exists = 1)
		BEGIN
			RAISERROR('Target path for object scripting doesn''t exists or it is incorrect.', 16, 1)
			RETURN;
		END;

	/*Activar el xp_cmdshell*/
	SELECT @advWasEnabled = CAST(value_in_use AS bit)
	FROM sys.configurations WHERE name = 'show advanced options';

	SELECT @xpWasEnabled = CAST(value_in_use AS bit)
	FROM sys.configurations WHERE name = 'xp_cmdshell';

	-- Habilitar lo necesario (solo si hace falta)
	IF (@advWasEnabled = 0)
	BEGIN
		PRINT 'enabling show advanced options...';
		EXEC sp_configure 'show advanced options', 1;
		RECONFIGURE WITH OVERRIDE;
	END

	IF (@xpWasEnabled = 0)
	BEGIN
		PRINT 'enabling xp_cmdshell temporary...';
		EXEC sp_configure 'xp_cmdshell', 1;
		RECONFIGURE WITH OVERRIDE;
	END

	/*
	Normalizacion de los separadores
	*/
	SET @base = REPLACE(@targetPath, '/', '\');
	SET @sep  = N'\';
	IF RIGHT(@base,1) <> @sep SET @base += @sep;

	SET @targetPath = CASE 
					WHEN CHARINDEX('/', @targetPath) > 0 THEN REPLACE (@targetPath, '\', '/')
					WHEN CHARINDEX('\', @targetPath) > 0 THEN REPLACE (@targetPath, '/', '\')
				END

	/*Verificar la existencia de las carpetas*/
	DECLARE route_constructor CURSOR FAST_FORWARD FOR
	SELECT name 
	FROM sys.databases
	WHERE database_id > 4
	AND state_desc = 'ONLINE';

	OPEN route_constructor
	FETCH NEXT FROM route_constructor INTO @dbName;

	WHILE @@FETCH_STATUS = 0 
		BEGIN
			SET @SQL = '
			SELECT DISTINCT '+QUOTENAME(@dbName, '''')+' AS databaseName,
				CASE o.type
					WHEN ''P''  THEN N''Procedures''
					WHEN ''V''  THEN N''Views''
					WHEN ''FN'' THEN N''Functions''
					WHEN ''IF'' THEN N''Functions''
					WHEN ''TF'' THEN N''Functions''
					WHEN ''TR'' THEN N''Triggers''
					WHEN ''U''  THEN N''Tables''
					WHEN ''SN'' THEN N''Synonyms''
					WHEN ''SQ'' THEN N''Sequences''
					WHEN ''SO'' THEN N''System Objects''
				ELSE N''Other''
            END AS objectType
        FROM ' + QUOTENAME(@dbName) + N'.sys.objects o
		WHERE o.is_ms_shipped = 0;';

		INSERT INTO @routeTable (databaseName, objectType)
		EXEC sp_executesql @SQL;

		SET @dbTargetPath = @targetPath+@dbName+@sep;

		IF NOT EXISTS (SELECT 1 FROM sys.dm_os_file_exists(@dbTargetPath) WHERE file_is_a_directory = 1)
			BEGIN
				EXEC master..xp_create_subdir @dbTargetPath;
			END;

		/* Crear carpetas por tipo */
		IF @perTypeSubFolder = 1
		BEGIN
			DECLARE c_types CURSOR LOCAL FAST_FORWARD FOR
				SELECT DISTINCT objectType
				FROM @routeTable
				WHERE databaseName = @dbName;
				OPEN c_types;
				FETCH NEXT FROM c_types INTO @objTypes;

				WHILE @@FETCH_STATUS = 0
				BEGIN
					SET @objTypesPath = @dbTargetPath + @objTypes + @sep;

					IF NOT EXISTS (SELECT 1 FROM sys.dm_os_file_exists(@objTypesPath) WHERE file_is_a_directory = 1)
						EXEC master..xp_create_subdir @objTypesPath;

					FETCH NEXT FROM c_types INTO @objTypes;
				END

				CLOSE c_types; 
				DEALLOCATE c_types;
		END

			FETCH NEXT FROM route_constructor INTO @dbName;
		END;

	CLOSE route_constructor; 
	DEALLOCATE route_constructor;
DECLARE object_constructor CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND state_desc = 'ONLINE';

OPEN object_constructor;
FETCH NEXT FROM object_constructor INTO @dbName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = N'
			;WITH modules AS (
			/*
			Construccion de los procedimientos almacenados
			*/
				SELECT
					DB_NAME() AS databaseName,
					s.name    AS schema_name,
					o.name    AS object_name,
					o.type    AS object_type,
					CASE o.type
						WHEN ''P''  THEN N''Procedures''
						WHEN ''V'' THEN N''Views''
						WHEN ''FN'' THEN N''Functions''
						WHEN ''IF'' THEN N''Functions''
						WHEN ''TF'' THEN N''Functions''
						WHEN ''TR'' THEN N''Triggers''
						WHEN ''SN'' THEN N''Synonyms''
						WHEN ''SQ'' THEN N''Sequences''
						WHEN ''SO'' THEN N''System Objects''
						ELSE N''Other''
					END       AS type_folder,
					sm.definition
				FROM {DB}.sys.sql_modules AS sm
				JOIN {DB}.sys.objects     AS o ON sm.object_id = o.object_id
				JOIN {DB}.sys.schemas     AS s ON o.schema_id  = s.schema_id
				WHERE o.is_ms_shipped = 0
			),
			cols AS (
			/*Informacion de las columnas*/
				SELECT
					c.object_id, c.column_id,
					c.name AS column_name,
					c.is_nullable, 
					c.is_computed,
					t.name AS type_name,
					c.max_length, 
					c.precision, 
					c.scale,
					dc.name AS default_name, 
					dc.definition AS default_definition,
					ic.seed_value, ic.increment_value,
					CASE WHEN ic.column_id IS NULL THEN 0 ELSE 1 END AS is_identity,
					cc.definition AS computed_definition
				FROM {DB}.sys.columns c
				JOIN {DB}.sys.tables  tb ON c.object_id = tb.object_id
				JOIN {DB}.sys.types   t  ON c.user_type_id = t.user_type_id
				LEFT JOIN {DB}.sys.default_constraints dc ON c.default_object_id = dc.object_id
				LEFT JOIN {DB}.sys.identity_columns    ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
				LEFT JOIN {DB}.sys.computed_columns    cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
				),

				pk AS (
					SELECT
						kc.parent_object_id AS object_id,
						kc.name AS constraint_name,
						STRING_AGG(QUOTENAME(c.name), '','') WITHIN GROUP (ORDER BY ic.key_ordinal) AS cols
					FROM {DB}.sys.key_constraints kc
					JOIN {DB}.sys.index_columns ic
					  ON kc.parent_object_id = ic.object_id
					 AND kc.unique_index_id  = ic.index_id
					JOIN {DB}.sys.columns c
					  ON ic.object_id = c.object_id
					 AND ic.column_id = c.column_id
					WHERE kc.type = ''PK''
					GROUP BY kc.parent_object_id, kc.name
				),

				col_lines AS (
				SELECT
					c.object_id, c.column_id,
					''    '' +
					QUOTENAME(c.column_name) + '' '' +
					CASE
						WHEN c.is_computed = 1
							THEN ''AS '' + c.computed_definition
						ELSE
							(
							  CASE
								WHEN c.type_name IN (''varchar'',''char'',''varbinary'',''binary'',''nvarchar'',''nchar'')
								   THEN c.type_name + ''('' +
										CASE WHEN c.max_length = -1 THEN ''MAX''
											 ELSE CAST(CASE WHEN c.type_name LIKE ''n%'' THEN c.max_length/2 ELSE c.max_length END AS varchar(10)) END
										+ '')''
								WHEN c.type_name IN (''decimal'',''numeric'')
								   THEN c.type_name + ''('' + CAST(c.precision AS varchar(10)) + '','' + CAST(c.scale AS varchar(10)) + '')''
								WHEN c.type_name IN (''time'',''datetime2'',''datetimeoffset'')
								   THEN c.type_name + ''('' + CAST(c.scale AS varchar(10)) + '')''
								ELSE c.type_name
							  END
							  + CASE WHEN c.is_identity = 1
									 THEN '' IDENTITY('' + CAST(c.seed_value AS varchar(50)) + '','' + CAST(c.increment_value AS varchar(50)) + '')''
									 ELSE '''' END
							  + CASE WHEN c.is_nullable = 1 THEN '' NULL'' ELSE '' NOT NULL'' END
							  + CASE WHEN c.default_definition IS NULL
									 THEN ''''
									 ELSE '' CONSTRAINT '' + QUOTENAME(c.default_name) + '' DEFAULT '' + c.default_definition END
							)
					END AS line
				FROM cols c
			),

			table_defs AS (
			SELECT
					t.object_id,
					s.name AS schema_name,
					t.name AS table_name,
					(
					  ''CREATE TABLE '' + QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name) + CHAR(13) +
					  ''('' + CHAR(13) +
					  STRING_AGG(cl.line, '','' + CHAR(13)) WITHIN GROUP (ORDER BY cl.column_id) +
					  CASE WHEN pk.object_id IS NULL
						   THEN ''''
						   ELSE '','' + CHAR(13) + ''    CONSTRAINT '' + QUOTENAME(pk.constraint_name) +
								'' PRIMARY KEY ('' + pk.cols + '')''
					  END +
					  CHAR(13) + '');''
					) AS definition
				FROM {DB}.sys.tables t
				JOIN {DB}.sys.schemas s ON t.schema_id = s.schema_id
				JOIN col_lines cl  ON t.object_id = cl.object_id
				LEFT JOIN pk       ON t.object_id = pk.object_id
				WHERE t.is_ms_shipped = 0
				GROUP BY t.object_id, s.name, t.name, pk.object_id, pk.constraint_name, pk.cols
			),
			tables AS (
				SELECT
					DB_NAME()  AS databaseName,
					td.schema_name,
					td.table_name AS object_name,
					''U''        AS object_type,
					N''Tables''  AS type_folder,
					td.definition
				FROM table_defs td
			),
			objs AS (
				SELECT databaseName, schema_name, object_name, object_type, type_folder, definition FROM modules
				UNION ALL
				SELECT databaseName, schema_name, object_name, object_type, type_folder, definition FROM tables
			),
			safe AS (
				SELECT
				  databaseName,
				  schema_name,
				  object_name,
				  type_folder,
				  definition,
				  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(schema_name, N''\'', N''_''), N''/'', N''_''), N'':'', N''_''), N''*'', N''_''), N''?'', N''_''), N''"'' , N''_''), N''<'', N''_''), N''>'', N''_''), N''|'', N''_'' ) AS schema_safe,
				  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(object_name, N''\'', N''_''), N''/'', N''_''), N'':'', N''_''), N''*'', N''_''), N''?'', N''_''), N''"'' , N''_''), N''<'', N''_''), N''>'', N''_''), N''|'', N''_'' ) AS object_safe
				FROM objs
			)

			SELECT 
			  schema_name,
			  object_name,
			  type_folder,
			  definition,
			  @p_base + @p_db + @p_sep +
			  CASE WHEN @p_perType = 1 THEN type_folder + @p_sep ELSE N'''' END 
			  + object_safe + N''.sql'' AS TargetPath
			FROM safe
			ORDER BY type_folder, schema_name, object_name'

	SET @SQL = REPLACE(@SQL, N'{DB}', QUOTENAME(@dbName));

    -- Insertamos el resultado en la tabla de lista de objetos
    INSERT INTO master..objectList(schemaName, objectName, typeFolder, [definition], TargetPath)
    EXEC sp_executesql
        @SQL,
        N'@p_base nvarchar(max), @p_db sysname, @p_sep nchar(1), @p_perType bit',
        @p_base = @base, @p_db = @dbName, @p_sep = @sep, @p_perType = @perTypeSubFolder;

    DECLARE c_dirs CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT
            @base + @dbName + @sep +
            CASE WHEN @perTypeSubFolder = 1 THEN typeFolder + @sep ELSE N'' END + @sep
        FROM master..objectList
        WHERE TargetPath LIKE @base + @dbName + @sep + '%';

    OPEN c_dirs;
    FETCH NEXT FROM c_dirs INTO @dir;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.dm_os_file_exists(@dir) WHERE file_is_a_directory = 1)
            EXEC master..xp_create_subdir @dir;
        FETCH NEXT FROM c_dirs INTO @dir;
    END
    CLOSE c_dirs; DEALLOCATE c_dirs;

    FETCH NEXT FROM object_constructor INTO @dbName;
END

CLOSE object_constructor; DEALLOCATE object_constructor;

WHILE 1=1
	BEGIN
		SELECT TOP 1
			@lastId = ObjectTypeId,
			@bcpTargetPath = TargetPath
		FROM master..objectList
		WHERE objectTypeId > @lastId
		AND processed = 0
		ORDER BY objectTypeId; 

		IF @@ROWCOUNT = 0 BREAK;

		SET @query = N'SELECT [definition] FROM master..objectList WHERE objectTypeId = '+CAST(@lastId AS VARCHAR);

		SET @cmd = 'cmd /c "';

		EXEC master.dbo.xp_fileexist @bcpTargetPath, @exists OUTPUT;

		IF @overWriteFiles = 1 AND @exists = 1
			SET @cmd += 'IF EXIST "' + @bcpTargetPath + '" del /f /q "' + @bcpTargetPath + '" && ';

		SET @cmd += '"' + @bcpExe + '" "' + REPLACE(@query,'"','""') + '" '
				  + 'queryout "' + @bcpTargetPath + '" '
				  + '-S ' + @serverName + ' -d master -T -w"';

		INSERT INTO @out
		EXEC @rc = master..xp_cmdshell @cmd;

		IF (@rc = 0)
		BEGIN
			UPDATE master..objectList
			SET processed = 1
			WHERE ObjectTypeId = @lastId;
		END
	END
	SELECT * FROM @out
	IF (@xpWasEnabled = 0)
	BEGIN
		PRINT 'Disabling xp_cmdshell...';
		EXEC sp_configure 'xp_cmdshell', 0;
		RECONFIGURE WITH OVERRIDE;
	END;

END;
