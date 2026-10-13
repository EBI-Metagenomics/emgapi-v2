"""The accessioning core: the hash contract, the file reader, the MGYP lookup and the staging transaction.

This package does not import Django, so that it can run outside emgapi-v2 later (for the gene
calling pipeline). It keeps its own psycopg connections, because it uses parallel lookups and binary COPY.
"""
