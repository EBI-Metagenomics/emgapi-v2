"""The accessioning core: the hash contract, reading the predicted genes, the MGYP lookup and the staging transaction.

This package does not import Django. The caller gives it the connections (see assembly_proteins.Connect):
in emgapi-v2, proteins.routers.mgyp_connection() gives the psycopg connection of the Django mgyp alias.
"""
