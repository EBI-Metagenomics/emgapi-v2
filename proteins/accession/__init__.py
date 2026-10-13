"""The accessioning core: the hash contract, the file reader, the MGYP lookup, the staging transaction and the FAA with MGYPs.

This package does not import Django. The caller gives it the connections (see accession.Connect):
in emgapi-v2, proteins.routers.mgyp_connection() gives the psycopg connection of the Django mgyp alias.
"""
