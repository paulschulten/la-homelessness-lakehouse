import duckdb
import pandas as pd

# point at your actual client.duckdb file
con = duckdb.connect('client.duckdb')

# replicate the ~/.duckdbrc startup sequence (not auto-run outside the CLI)
con.sql("INSTALL iceberg; LOAD iceberg;")
con.sql("CREATE SECRET IF NOT EXISTS (TYPE s3, PROVIDER credential_chain, CHAIN 'config');")
con.sql("""
    ATTACH IF NOT EXISTS '277607772876' AS glue_catalog (
        TYPE iceberg,
        ENDPOINT 'glue.us-east-2.amazonaws.com/iceberg',
        AUTHORIZATION_TYPE 'sigv4'
    );
""")

# pull the joined view
df = con.sql("SELECT * FROM acs_pit_joined").df()
print(df.columns.tolist())
# sanity check before anything else
print(f"rows: {len(df)}, cols: {len(df.columns)}")
assert len(df) > 0, "join returned no rows — check tract_fips format / year alignment first"

# columns to normalize (numeric, not id/year/population/target)
exclude = ['tract_fips', 'year', 'total_population', 'totpeople']
numeric_cols = [c for c in df.select_dtypes('number').columns if c not in exclude]

# blanket normalize by total_population — first pass, not final
rates = df[numeric_cols].div(df['total_population'], axis=0)
rates.columns = [f"{c}_rate" for c in numeric_cols]

df_norm = pd.concat([df[['tract_fips', 'year']], rates], axis=1)
df_norm['totpeople_rate'] = df['totpeople'] / df['total_population']

# correlate everything against the target rate
corrs = df_norm.drop(columns=['tract_fips', 'year']).corrwith(df_norm['totpeople_rate']).dropna()
top = corrs.abs().sort_values(ascending=False).head(30)

print(top)
top.to_csv('acs_pit_top_correlations.csv')