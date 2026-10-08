from django.db.models import CharField, Func


class PreferredENAAccession(Func):
    """Resolve an ENA accession using the model's preferred-accession semantics."""

    output_field = CharField()

    def __init__(self, expression, preferred_accession_pattern: str):
        self.preferred_accession_pattern = preferred_accession_pattern
        super().__init__(expression)

    def as_sql(self, compiler, connection, **extra_context):
        array_sql, array_params = compiler.compile(self.source_expressions[0])
        sql = f"""COALESCE(
            (
                SELECT accession
                FROM unnest({array_sql}) WITH ORDINALITY
                    AS accessions(accession, position)
                WHERE accession ~ %s
                ORDER BY position
                LIMIT 1
            ),
            ({array_sql})[1]
        )"""
        return sql, [
            *array_params,
            self.preferred_accession_pattern,
            *array_params,
        ]
