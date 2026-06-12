from urllib.parse import urljoin

from django.conf import settings
from django.contrib.syndication.views import Feed

from analyses.models import Study


class LatestStudiesFeed(Feed):
    title = "MGnify Latest Studies"
    link = "/studies/"
    description = "Latest studies analysed by MGnify pipelines"

    def items(self):
        return Study.public_objects.order_by("-updated_at")[
            : settings.EMG_CONFIG.distribution.latest_studies_feed_count
        ]

    def item_title(self, item: Study):
        return f"{item.accession}: {item.title}"

    def item_description(self, item: Study):
        return f"{' | '.join(item.ena_accessions)}: {item.metadata.get('study_description', 'No description available.')}"

    def item_link(self, item: Study):
        return urljoin(
            settings.EMG_CONFIG.distribution.studies_url_root_for_permalinks,
            item.accession,
        )

    def item_author_name(self, item: Study):
        return item.metadata.get("center_name", "No submission centre available.")

    def item_pubdate(self, item: Study):
        return item.created_at

    def item_updateddate(self, item: Study):
        return item.updated_at

    def item_categories(self, item: Study):
        return [item.biome.biome_name]
