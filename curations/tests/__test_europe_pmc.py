from curations.europe_pmc import group_epmc_publication_annotations


def test_group_merges_case_insensitive_text_when_mentions_are_interleaved():
    response = group_epmc_publication_annotations(
        {
            "annotations": [
                {"type": "Host", "exact": "Alpha", "tags": []},
                {"type": "Host", "exact": "Beta", "tags": []},
                {"type": "Host", "exact": "alpha", "tags": []},
            ]
        }
    )

    host_group = response.other[0]
    assert len(host_group.annotations) == 2
    assert len(host_group.annotations[0].mentions) == 2
