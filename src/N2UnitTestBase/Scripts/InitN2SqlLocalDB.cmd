::****************************
:: sqllocaldb の準備
::****************************

:: if you want to reset N2SqlLocalDB, remove '::' comment out abd run once.
:: SET N2_SQL_LOCAL_DB_RESET=1

IF DEFINED N2_SQL_LOCAL_DB_RESET (
    sqllocaldb stop N2SqlLocalDB
    sqllocaldb delete N2SqlLocalDB
    DEL /F /Q %USERPROFILE%\\N2SqlLocalDB.*.mdf
    DEL /F /Q %USERPROFILE%\\N2SqlLocalDB.*.ldf
)

sqllocaldb create N2SqlLocalDB -s
sqllocaldb info N2SqlLocalDB
