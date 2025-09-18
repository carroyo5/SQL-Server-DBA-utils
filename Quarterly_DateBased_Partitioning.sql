/*
====================================================================
 Script:    Auto-Partitioning by Quarter (Filegroups + Partitioning)
 Author:    Cristhian Arroyo
 Descripción:
   Este script automatiza la creación de filegroups, archivos .ndf,
   funciones de partición y esquemas de partición en SQL Server, 
   organizando los datos de tablas seleccionadas por trimestres.

   Está diseñado específicamente para particionar tablas 
   basadas en columnas de FECHA (datetime, date, datetime2, etc.).

 Funcionalidad:
   1. Identifica tablas grandes con columnas de tipo fecha.
   2. Calcula el rango de fechas (mínimo y máximo).
   3. Genera secuencia de trimestres dentro del rango encontrado.
   4. Crea dinámicamente filegroups y archivos físicos (.ndf).
   5. Define o recrea la Partition Function y Partition Scheme.
   6. Asocia cada partición a un filegroup distinto.

 Parámetros configurables:
   - @MinRowTreshold       → Umbral mínimo de filas.
   - @PathName             → Carpeta donde se crean los archivos .ndf.
   - @FileSize / @FileGrowth / @FileMaxSize → Configuración de archivos.
   - @FileGroupPrefix      → Prefijo para nombrar los filegroups.
   - @SelectedTablesList   → Lista de tablas a particionar.
   - @pfName / @psName     → Nombre de Partition Function y Scheme.

 Uso recomendado:
   Ejecutar en bases de datos con tablas de gran volumen 
   (logs, auditoría, histórico) donde se requiera partición temporal.

====================================================================
*/

USE [Database Name]
GO
-- Definicion de parametros de configuracion de files y filegroup
DECLARE 
    @MinRowTreshold INT      = 1000000,                  -- umbral minimo de filas
    @PathName NVARCHAR(500)  = N'D:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\Data',
    @FileSize NVARCHAR(16)   = '100',                     -- tamano inicial en MB
    @FileGrowth NVARCHAR(16) = '54',                      -- crecimiento en MB
    @FileGroupPrefix NVARCHAR(20) = 'FG_LOGGING',        -- prefijo para filegroups
    @FileMaxSize NVARCHAR(20)     = 'UNLIMITED', -- tamano maximo
	@SelectedTablesList VARCHAR(1000) = ' '; --Tablas para particionar
	
-- Parametros de Partition Function
DECLARE 
    @pfName     VARCHAR(200) = N'[Nombre del File Group]',
    @pfDataType VARCHAR(200) = 'DATETIME';

-- Parametros de Partition Scheme
DECLARE 
    @psName VARCHAR(200) = N'[Nombre del Partition Scheme]';

-- Variables internas para el proceso
DECLARE
    @MinDate DATETIME,
    @MaxDate DATETIME,
	@ExtendedMaxDate DATETIME,
	@CurrentQuarterStart DATETIME,
    @SQL NVARCHAR(MAX),
    @Label            NVARCHAR(20),
    @FileGroupName    NVARCHAR(128),
    @LogicalFileName  NVARCHAR(128),
    @PhysicalFileName NVARCHAR(512),
    @PathSeparator NCHAR(1) = CASE
        WHEN @PathName LIKE '%\%' AND @PathName NOT LIKE '%/%' THEN '\'
        WHEN @PathName LIKE '%/%' AND @PathName NOT LIKE '%\%' THEN '/'
        WHEN @PathName LIKE '%\%' AND @PathName LIKE '%/%' THEN RIGHT(@PathName,1) COLLATE Latin1_General_BIN
        ELSE '\'
      END,
    -- Variables para cursor
    @CurSchemaName   VARCHAR(200),
    @CurTableName    VARCHAR(200),
    @CurColumnName   VARCHAR(200),
    @CurObjectId     INT;

-- Tabla temporal para almacenar tablas a particionar
DECLARE @TablesForPartition TABLE (
    TableId       INT PRIMARY KEY,
    SchemaName    VARCHAR(200),
    TableName     VARCHAR(200),
    PartitionColumn VARCHAR(200),
	ColumnDataType VARCHAR(200),
    MinDateTime   DATETIME,
    MaxDateTime   DATETIME
);

-- Tabla temporal para almacenar trimestres y filegroups
DECLARE @QuartersSeq TABLE (
    Seq           INT IDENTITY(1,1) PRIMARY KEY,
    QuarterLabel  VARCHAR(100),
    QuarterStart  DATETIME,
    QuarterEnd    DATETIME,
    FileGroupName SYSNAME
);

-- Paso 1: Encontrar todas las tablas con columna datetime y filas > umbral
WITH SelectedTables AS (
    SELECT 
        t.object_id,
        t.name AS TableName,
        SCHEMA_NAME(t.schema_id) AS SchemaName,
        sc.name AS ColumnName,
		ty.name AS ColumnType
    FROM sys.tables t
    JOIN sys.dm_db_partition_stats ps ON t.object_id = ps.object_id
    JOIN sys.columns sc ON t.object_id = sc.object_id
    JOIN sys.types ty ON sc.user_type_id = ty.user_type_id
    WHERE ty.name IN ('date','datetime','datetime2','smalldatetime','datetimeoffset') --Tipo(s) de datos admitidos
	AND (@SelectedTablesList IS NULL OR t.name IN (SELECT TRIM(value)
      FROM STRING_SPLIT(@SelectedTablesList, ',')))
    GROUP BY t.object_id, t.name, sc.name, ty.name, t.schema_id
    HAVING SUM(CASE WHEN ps.index_id < 2 THEN ps.row_count ELSE 0 END) > @MinRowTreshold
),

-- Paso 2: Si hay varias columnas datetime, elegir una (row_number)
FilteredTables AS (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY TableName, SchemaName ORDER BY (SELECT NULL)) AS rn,
        object_id,
        SchemaName,
        TableName,
        ColumnName,
		ColumnType
    FROM SelectedTables
)
-- Insertar en TablesForPartition solo la fila rn = 1
INSERT INTO @TablesForPartition (TableId, SchemaName, TableName, PartitionColumn, ColumnDataType)
SELECT object_id, SchemaName, TableName, ColumnName, ColumnType
FROM FilteredTables
WHERE rn = 1;

-- Paso 3: Calcular minimo y maximo por tabla usando cursor
DECLARE table_cur CURSOR FOR
SELECT TableId, SchemaName, TableName, PartitionColumn
FROM @TablesForPartition;

OPEN table_cur;
FETCH NEXT FROM table_cur INTO @CurObjectId, @CurSchemaName, @CurTableName, @CurColumnName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Obtener min y max de la columna datetime
    SET @SQL = N'SELECT @MinOut = MIN(' + QUOTENAME(@CurColumnName) + '), 
                        @MaxOut = MAX(' + QUOTENAME(@CurColumnName) + ')
                 FROM ' + QUOTENAME(@CurSchemaName) + '.' + QUOTENAME(@CurTableName) + ';';
    EXEC sp_executesql
        @SQL,
        N'@MinOut DATETIME OUTPUT, @MaxOut DATETIME OUTPUT',
        @MinOut = @MinDate OUTPUT,
        @MaxOut = @MaxDate OUTPUT;

    -- Normalizar fechas a inicio de mes y fin de mes
    UPDATE @TablesForPartition
    SET 
        MinDateTime = DATEFROMPARTS(YEAR(CONVERT(DATE,@MinDate)), MONTH(CONVERT(DATE,@MinDate)), 1),
        MaxDateTime = CONVERT(DATE, @MaxDate)
    WHERE TableId = @CurObjectId;

    FETCH NEXT FROM table_cur INTO @CurObjectId, @CurSchemaName, @CurTableName, @CurColumnName;
END;

CLOSE table_cur;
DEALLOCATE table_cur;

-- Paso 4: Calcular rango global de fechas
SELECT
    @MinDate = MIN(MinDateTime),
    @MaxDate = MAX(MaxDateTime)
FROM @TablesForPartition;

SET @CurrentQuarterStart = DATEADD(QUARTER,
            DATEDIFF(QUARTER, 0, @MaxDate),
            0);

SET @ExtendedMaxDate = DATEADD(SECOND, -1,
        DATEADD(QUARTER, 2, @CurrentQuarterStart));

-- Paso 5: Generar secuencia de meses entre MinDate y MaxDate
WITH Months AS (
    SELECT CAST(DATEFROMPARTS(YEAR(@MinDate), MONTH(@MinDate), 1) AS DATETIME) AS FirstOfMonth
    UNION ALL
    SELECT DATEADD(MONTH, 1, FirstOfMonth)
    FROM Months
    WHERE DATEADD(MONTH, 1, FirstOfMonth) <= @ExtendedMaxDate
),
-- Paso 6: Agrupar meses en trimestres
Quarters AS (
    SELECT DISTINCT
        'Q' + CAST(DATEPART(QUARTER, FirstOfMonth) AS VARCHAR(1))
        + '_' + CAST(YEAR(FirstOfMonth) AS VARCHAR(4)) AS QuarterLabel,

        MIN(FirstOfMonth) OVER (PARTITION BY DATEPART(YEAR, FirstOfMonth), DATEPART(QUARTER, FirstOfMonth)) AS QuarterStart,
        DATEADD(SECOND, -1, DATEADD(MONTH, 3,
            MIN(FirstOfMonth) OVER (PARTITION BY DATEPART(YEAR, FirstOfMonth), DATEPART(QUARTER, FirstOfMonth))
        )) AS QuarterEnd
    FROM Months
),
-- Paso 7: Asignar filegroup por trimestre
FGQuarters AS (
    SELECT
        QuarterLabel,
        QuarterStart,
        QuarterEnd,
        @FileGroupPrefix
        + (CASE WHEN RIGHT(@FileGroupPrefix,1) LIKE '[A-Za-z0-9]' THEN '_' ELSE '' END)
        + QuarterLabel AS FileGroupName
    FROM Quarters
)
INSERT INTO @QuartersSeq (QuarterLabel, QuarterStart, QuarterEnd, FileGroupName)
SELECT QuarterLabel, QuarterStart, QuarterEnd, FileGroupName
FROM FGQuarters
ORDER BY QuarterStart;

-- Ver tabla de trimestres generada
SELECT * FROM @QuartersSeq;

-- Paso 8: Crear filegroups y archivos .ndf
DECLARE create_fg_cur CURSOR FOR
SELECT QuarterLabel, FileGroupName
FROM @QuartersSeq
ORDER BY Seq;

OPEN create_fg_cur;
FETCH NEXT FROM create_fg_cur INTO @Label, @FileGroupName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Construir nombre logico y fisico de archivo
    SET @LogicalFileName = (CASE WHEN DB_NAME() LIKE '%[A-Z]%' THEN (
        SELECT '' + SUBSTRING(DB_NAME(), v.number, 1)
        FROM master..spt_values v
        WHERE v.type = 'P'
          AND v.number BETWEEN 1 AND LEN(DB_NAME())
          AND SUBSTRING(DB_NAME(), v.number, 1) COLLATE Latin1_General_BIN LIKE '[A-Z]'
        ORDER BY v.number
        FOR XML PATH('')) ELSE DB_NAME() END) + '_' + @Label;

    SET @PhysicalFileName = @PathName 
        + CASE WHEN RIGHT(@PathName,1) IN ('\', '/') THEN '' ELSE @PathSeparator END
        + @LogicalFileName + N'.ndf';

    -- Generar comando para crear filegroup y archivo
    SET @SQL = N'
    IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE name = ' + QUOTENAME(@FileGroupName,'''') + N')
    BEGIN
        ALTER DATABASE ' + QUOTENAME(DB_NAME()) + N'
        ADD FILEGROUP ' + QUOTENAME(@FileGroupName) + N';

        ALTER DATABASE ' + QUOTENAME(DB_NAME()) + N'
        ADD FILE (
            NAME = ' + QUOTENAME(@LogicalFileName,'''') + N',
            FILENAME = ' + QUOTENAME(@PhysicalFileName,'''') + N',
            SIZE = ' + @FileSize + N'MB,
            FILEGROWTH = ' + @FileGrowth + N'MB,
            MAXSIZE = ' + (CASE WHEN @FileMaxSize <> 'UNLIMITED' THEN @FileMaxSize + N'GB' ELSE @FileMaxSize END) + N'
        ) TO FILEGROUP ' + QUOTENAME(@FileGroupName) + N';
    END';

    PRINT @SQL;
    EXEC sp_executesql @SQL;

    FETCH NEXT FROM create_fg_cur INTO @Label, @FileGroupName;
END;

CLOSE create_fg_cur;
DEALLOCATE create_fg_cur;

-- Paso 9: Crear o recrear Partition Function
/*
Si tu PF tiene N valores, generas N particiones.

Por tanto tu PS debe listar N+1 filegroups
*/
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = @pfName)
BEGIN
    SET @SQL = 'DROP PARTITION FUNCTION ' + QUOTENAME(@pfName);
    EXEC sp_executesql @SQL;
END

SELECT 
    @SQL = N'CREATE PARTITION FUNCTION ' + QUOTENAME(@pfName)
         + N'(' + @pfDataType + N') AS RANGE RIGHT FOR VALUES ('
         + STRING_AGG(QUOTENAME(CONVERT(CHAR(19), QuarterEnd,120), ''''), N',')
         + N');'
FROM @QuartersSeq 
WHERE Seq < (SELECT MAX(Seq) FROM @QuartersSeq);

PRINT @SQL;
EXEC sp_executesql @SQL;

-- Paso 10: Crear o recrear Partition Scheme
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = @psName)
BEGIN
    SET @SQL = 'DROP PARTITION SCHEME ' + QUOTENAME(@psName);
    EXEC sp_executesql @SQL;
END

SELECT 
    @SQL = N'CREATE PARTITION SCHEME ' + QUOTENAME(@psName)
         + N' AS PARTITION ' + QUOTENAME(@pfName) + N' TO ('
         + STRING_AGG(QUOTENAME(FileGroupName), N', ') WITHIN GROUP (ORDER BY Seq)
         + N');'
FROM @QuartersSeq;

PRINT @SQL;
EXEC sp_executesql @SQL;

