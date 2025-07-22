# load1.py
def load_spark_dfs_to_sqlserver(config, spark_dfs):
    sql = config['SQL_SERVER']

    jdbc_url = f"jdbc:sqlserver://{sql['server']};databaseName={sql['database']};encrypt=true;trustServerCertificate=true"
 
    props = {
        "user": sql['username'],
        "password": sql['password'],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    for name, df in spark_dfs.items():
        df.write.mode("overwrite").jdbc(
            url=jdbc_url,
            table=f"[{name}]",
            properties=props
        )
        print(f"Loaded table: {name} to SQL Server")

