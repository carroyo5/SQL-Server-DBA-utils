SELECT 
    FORMAT(CAST(sjob.date_created AS smalldatetime), 'yyyy-MM-dd hh:mm tt') AS 'Fecha de creación',
    sjob.name AS 'Nombre del Job',
    CASE sjob.notify_level_eventlog
        WHEN 0 THEN 'Nunca'
        WHEN 1 THEN 'Cuando se termina correctamente'
        WHEN 2 THEN 'Cuando se produce un Error'
        WHEN 3 THEN 'Cada vez que se complete el Job'
    END AS 'Notificación en el EventViewer',
    CASE sjob.notify_level_email
        WHEN 0 THEN 'Nunca'
        WHEN 1 THEN 'Cuando se termina correctamente'
        WHEN 2 THEN 'Cuando se produce un Error'
        WHEN 3 THEN 'Cada vez que se complete el Job'
    END AS 'Notificación en el Correo',
    jobcategories.name AS 'Categoría',
    ISNULL(operators.name, 'N/A') AS 'Nombre del operador',
    ISNULL(operators.email_address, 'N/A') AS 'Correo',
    schedules.name AS 'Nombre del Horario',
    CASE freq_type 
        WHEN 1 THEN 'Una sola vez'
        WHEN 4 THEN 'Diaria'
        WHEN 8 THEN 'Semanal'
        WHEN 16 THEN 'Mensual'
        WHEN 32 THEN 'Mensual (Relativo a la frecuencia de intervalo)'
        WHEN 64 THEN 'Continuo'
        WHEN 128 THEN 'Ejecución durante inactividad' 
    END AS 'Frecuencia', 
    FORMAT(
        CAST(
            '1900-01-01 ' + 
            STUFF(STUFF(RIGHT('000000' + CAST(schedules.active_start_time AS VARCHAR(6)), 6), 5, 0, ':'), 3, 0, ':')
        AS DATETIME), 'hh:mm:ss tt') AS 'Hora de inicio',
    FORMAT(
        CAST(
            '1900-01-01 ' + 
            STUFF(STUFF(RIGHT('000000' + CAST(schedules.active_end_time AS VARCHAR(6)), 6), 5, 0, ':'), 3, 0, ':')
        AS DATETIME), 'hh:mm:ss tt') AS 'Hora de fin',
    CASE 
        WHEN schedules.freq_type IN (4, 8) THEN 
            CASE 
                WHEN schedules.freq_subday_type = 1 THEN 'Una vez al día'
                WHEN schedules.freq_subday_type = 2 THEN CONCAT('Cada ', schedules.freq_subday_interval, ' segundos')
                WHEN schedules.freq_subday_type = 4 THEN CONCAT('Cada ', schedules.freq_subday_interval, ' minutos')
                WHEN schedules.freq_subday_type = 8 THEN CONCAT('Cada ', schedules.freq_subday_interval, ' horas')
                ELSE 'Intervalo desconocido'
            END
        ELSE 'Sin repeticiones o una vez'
    END AS 'Intervalo de Ejecución'
FROM msdb.dbo.sysjobs AS sjob
INNER JOIN msdb.dbo.syscategories AS jobcategories 
    ON jobcategories.category_id = sjob.category_id
LEFT JOIN msdb.dbo.sysoperators AS operators 
    ON operators.id = sjob.notify_email_operator_id
INNER JOIN msdb.dbo.sysjobschedules AS jobschedules 
    ON jobschedules.job_id = sjob.job_id
INNER JOIN msdb.dbo.sysschedules AS schedules 
    ON schedules.schedule_id = jobschedules.schedule_id
WHERE sjob.enabled = 1
ORDER BY sjob.name;
