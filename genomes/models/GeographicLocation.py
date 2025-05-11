from django.db import models

class GeographicLocation(models.Model):

    name = models.CharField(db_column='CONTINENT', max_length=80, unique=True)

    class Meta:
        db_table = 'geographic_range'

    def __str__(self):
        return self.name