::****************************
:: sqllocaldb の準備
::****************************

:: if you want to reset N2SqlLocalDB, remove '::' comment out abd run once.
:: SET N2_SQL_LOCAL_DB_RESET=1

IF DEFINED N2_SQL_LOCAL_DB_RESET (
    sqllocaldb stop N2SqlLocalDB0
    sqllocaldb delete N2SqlLocalDB0
    DEL /F /Q %USERPROFILE%\\N2SqlLocalDB0.*.mdf
    DEL /F /Q %USERPROFILE%\\N2SqlLocalDB0.*.ldf
)

sqllocaldb create N2SqlLocalDB0 -s
sqllocaldb info N2SqlLocalDB0
