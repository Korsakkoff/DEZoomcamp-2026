import duckdb

con = duckdb.connect("open_library_pipeline.duckdb")

# Switch to the dlt dataset schema
con.execute("SET search_path = open_library_data")

# Now list tables
#print(con.execute("SHOW TABLES").fetchall())

# Inspect the books table (adjust name if SHOW TABLES shows something like 'books' or similar)
print(con.execute("SELECT * FROM books LIMIT 10").fetchdf())