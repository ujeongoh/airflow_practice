
def get_redshift_schema(hook, schema, table):
    df = hook.get_pandas_df(f"""
            SELECT 
                column_name,                
            FROM information_schema.columns
            WHERE table_schema = '{schema}' 
            AND table_name = '{table}'
            ORDER BY ordinal_position""")
    ret = []

    for _, row in df.iterrows():
        ret.append(row['column_name'])
        
    return ret