#!/usr/bin/env python

from dotenv import load_dotenv
from scrapy_delta_crawling import scrapy_delta_crawling
import pytest
import tempfile
import json
from scrapy import Spider

load_dotenv()


previous_items = [
    {"pk_field": "1", "value_field": "data1"},
    {"pk_field": "2", "value_field": "data2"},
]

with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
    json.dump(previous_items, f)
    previous_items_file_path = f.name


def custom_process_differences(self, item):
    pass


previous_file_fixtures = [
    dict(
        primary_key_fields=["pk_field"],
        data_diff_field="data_diff",
        keep_data_diff_field=True,
        sh_apikey=None,
        diff_functions={},
        previous_items_file_path=previous_items_file_path,
        collection_name=None,
        fields_to_compare=["value_field"],
        expected_diff_field={
            "diff": {
                "value_field": {
                    "previous": "data1",
                    "current": "data3",
                    "diff": None,
                    "diff_function": "identity",
                },
            },
            "is_new": False,
        },
        item_to_process={"pk_field": "1", "value_field": "data3"},
    ),
    dict(
        primary_key_fields=["pk_field"],
        data_diff_field="data_diff",
        keep_data_diff_field=True,
        sh_apikey=None,
        diff_functions={},
        previous_items_file_path=previous_items_file_path,
        collection_name=None,
        fields_to_compare=["value_field"],
        expected_diff_field={"diff": {}, "is_new": True},
        item_to_process={"pk_field": "4", "value_field": "data3"},
    ),
]


@pytest.fixture(params=previous_file_fixtures)
def previous_file_fixture(request):
    return request.param


def test_dummy(previous_file_fixture):
    expected_diff_field = previous_file_fixture.pop("expected_diff_field")
    item_to_process = previous_file_fixture.pop("item_to_process")

    pipeline = scrapy_delta_crawling.DeltaCrawlingPipeline(**previous_file_fixture)
    pipeline.open_spider(Spider(name="dummy"))
    processed_item = pipeline.process_item(item_to_process, None)

    assert (
        processed_item[previous_file_fixture["data_diff_field"]] == expected_diff_field
    )
