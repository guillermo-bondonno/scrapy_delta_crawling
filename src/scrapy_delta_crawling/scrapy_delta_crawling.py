from typing import Union
import copy
from scrapy.exceptions import DropItem
from scrapinghub import ScrapinghubClient
from typing import Any
import json
import os


class DeltaCrawlingPipeline:

    # TODO: assert that the spider has a pk attr or setting list[any]

    previous_items = {}

    def __init__(
        self,
        primary_key_fields: list,
        data_diff_field: str,
        keep_data_diff_field: bool,
        sh_apikey: str,
        diff_functions: dict,
        previous_items_file_path: str,
        collection_name: str,
        fields_to_compare: list,
    ):
        self.primary_key_fields = primary_key_fields
        self.DATA_DIFF_FIELD = data_diff_field
        self.KEEP_DATA_DIFF_FIELD = keep_data_diff_field
        self.sh_client = ScrapinghubClient(sh_apikey)
        self.diff_functions = self.build_comparators(diff_functions)
        self.previous_items_file_path = previous_items_file_path
        self.collection_name = collection_name
        self.fields_to_compare = fields_to_compare

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            primary_key_fields=crawler.settings.get(
                "DELTA_CRAWL_PRIMARY_KEY_FIELDS", None
            ),
            data_diff_field=crawler.settings.get("DELTA_CRAWL_DIFF_FIELD", "data_diff"),
            keep_data_diff_field=crawler.settings.get(
                "DELTA_CRAWL_KEEP_DIFF_FIELD", True
            ),
            collection_name=crawler.settings.get("DELTA_CRAWL_COLLECTION_NAME", None),
            sh_apikey=crawler.settings.get("SH_APIKEY", None),
            diff_functions=crawler.settings.get("DELTA_CRAWL_DIFF_FUNCTIONS", {}),
            previous_items_file_path=crawler.settings.get(
                "DELTA_CRAWL_PREVIOUS_ITEMS_FILE_PATH", None
            ),
            fields_to_compare=crawler.settings.get("DELTA_CRAWL_FIELDS_TO_COMPARE", []),
        )

    def open_spider(self, spider):
        self.spider_name = spider.name
        if getattr(spider, "primary_key_fields", None) is not None:
            self.primary_key_fields = spider.primary_key_fields

        if self.primary_key_fields is None:
            raise ValueError(
                "primary_key_fields (['field_1', 'field_2']) must be set in the setting "
                "(PRIMARY_KEY_FIELDS) or spider attr primary_key_fields."
            )

        self.load_previous_items()

    @staticmethod
    def items_from_file(file_path: str):
        if file_path.endswith(".json"):
            with open(file_path, "r") as f:
                for item in json.load(f):
                    yield item

    def items_from_previous_job(self):
        project_id = os.environ.get("SH_JOBKEY").split("/")[0]
        # get the latest finished job with the self.spider_name name
        job = next(
            self.sh_client.get_project(project_id).jobs.iter(
                spider=self.spider_name, state="finished"
            )
        )
        for item in self.sh_client.get_job(job["key"]).items.iter():
            yield item

    def items_from_collection(self):
        project_id = os.environ.get("SH_JOBKEY").split("/")[0]
        assert project_id, "SH_JOBKEY not set"
        project = self.sh_client.get_project(project_id)

        collection_found = False
        for collection in project.collections.iter():
            if collection["name"] == self.collection_name:
                collection_getter = {
                    "s": "get_store",
                    "cs": "get_cached_store",
                    "vs": "get_versioned_store",
                    "vcs": "get_versioned_cached_store",
                }[collection["type"]]
                self.collection_object = getattr(project, collection_getter)(
                    collection["name"]
                )
                collection_found = True
                for item in self.collection_object.iter():
                    yield item
        if not collection_found:
            raise ValueError(
                f"Collection {self.collection_name} not found in project {project_id}"
            )

    def only_fields_to_compare(self, item):
        return {field: item.get(field) for field in self.fields_to_compare}

    def load_previous_items(self):
        file_path = self.previous_items_file_path

        if bool(self.collection_name):
            items_iterator = self.items_from_collection()
            project_id = os.environ.get("SH_JOBKEY", "").split("/")[0]
            items_iterator = (
                self.sh_client.get_project(project_id)
                .collections.get_store(self.collection_name)
                .iter()
            )
        elif bool(file_path):
            items_iterator = self.items_from_file(file_path)
        else:
            try:
                items_iterator = self.items_from_previous_job()
            except Exception:
                raise ValueError(
                    "Couldn't fetch items from previous finished job. Make "
                    "sure the job exists and is finished or either "
                    "DELTA_CRAWL_COLLECTION_NAME or DELTA_CRAWL_PREVIOUS_ITEMS_FILE_PATH (not both)"
                )

        self.previous_items = {}
        for item in items_iterator:
            item.pop("_key", None)
            self.previous_items[self.get_pk(item)] = self.only_fields_to_compare(item)

    def get_pk(self, item):
        return tuple(item.get(field) for field in self.primary_key_fields)

    def _get_previous_item(self, primary_key):
        return self.previous_items.get(primary_key)

    @staticmethod
    def default_diff_function(previous, current):
        return previous != current, None

    def build_comparators(self, diff_functions):
        # TODO: build the functions from the settings import paths if str
        result = {}
        for key, value in diff_functions.items():
            if isinstance(value, str):
                pass
            elif callable(value):
                result[key] = value
        return result

    def compare_fields(self, field, previous, current) -> tuple[bool, Any, Any]:
        diff_function = self.diff_functions.get(field)

        diff_name = "identity" or diff_function.__name__
        diff_function = diff_function or self.default_diff_function

        is_different, distance = diff_function(previous, current)

        return is_different, distance, diff_name

    def populate_data_diff_field(
        self, item: dict, previous_item: Union[dict, None]
    ) -> Union[dict, None]:
        result = {}
        result["diff"] = {}
        if previous_item is None:
            result["is_new"] = True
            item[self.DATA_DIFF_FIELD] = result
            return
        result["is_new"] = False

        for field, value in copy.deepcopy(item).items():
            if field == self.DATA_DIFF_FIELD:
                continue
            previous = previous_item.get(field)
            is_different, diff_value, diff_function = self.compare_fields(
                field, previous, value
            )
            if is_different:
                result["diff"][field] = {
                    "previous": previous,
                    "current": value,
                    "diff": diff_value,
                    "diff_function": diff_function,
                }

        item[self.DATA_DIFF_FIELD] = result

    def process_differences(self, item: dict) -> bool:
        """
        Decide what to do with the item based on the info in item[DATA_DIFF_FIELD]
        Default beahviour is to drop the item if it wasn't modified
        """
        if (
            not item[self.DATA_DIFF_FIELD]["diff"]
            and not item[self.DATA_DIFF_FIELD]["is_new"]
        ):
            raise DropItem("Item already seen and not modified. Dropping.")

    def update_item_in_collection(self, item: dict):
        assert hasattr(self, "collection_object"), "Collection not set"

    def process_item(self, item, spider):
        # TODO: handle items that are not dicts
        # convert to dict and convert back to the original type before returning
        primary_key = self.get_pk(item)
        previous_item = self._get_previous_item(primary_key)

        self.populate_data_diff_field(item, previous_item)

        self.process_differences(item)

        if bool(self.collection_name):
            self.update_item_in_collection(item)

        if not self.KEEP_DATA_DIFF_FIELD:
            del item[self.DATA_DIFF_FIELD]
        return item
