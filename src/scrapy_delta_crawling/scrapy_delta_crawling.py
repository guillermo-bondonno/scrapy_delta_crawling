from typing import Union
import copy
from scrapy.exceptions import DropItem
from scrapinghub import ScrapinghubClient


class DeltaCrawlingPipeline:

    # TODO: assert that the spider has a pk attr or setting list[any]

    previous_items = {}

    def __init__(
        self, primary_key_fields, data_diff_field, keep_data_diff_field, sh_apikey
    ):
        self.primary_key_fields = primary_key_fields
        self.DATA_DIFF_FIELD = data_diff_field
        self.KEEP_DATA_DIFF_FIELD = keep_data_diff_field
        self.sh_client = ScrapinghubClient(sh_apikey)

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
            collection_name=crawler.settings.get(
                "DELTA_CRAWL_COLLECTION_NAME", crawler.spider.name
            ),
            sh_apikey=crawler.settings.get("SH_APIKEY", None),
        )

    def open_spider(self, spider):
        if getattr(spider, "primary_key_fields", None) is not None:
            self.primary_key_fields = spider.primary_key_fields

        if self.primary_key_fields is None:
            raise ValueError(
                "primary_key_fields (['field_1', 'field_2']) must be set in the setting "
                "(PRIMARY_KEY_FIELDS) or spider attr primary_key_fields."
            )

        self.load_collection()

    def load_collection(self):
        self.collection = self.sh_client.collections.get_collection(
            self.collection_name
        )
        self.previous_items = {
            self.get_pk(item): item
            # TODO check if iter() returns items or collection items ?
            for item in self.collection.iter()
        }

    def get_pk(self, item):
        return tuple(item.get(field) for field in self.primary_key_fields)

    def _get_previous_item(self, primary_key):
        return self.previous_items.get(primary_key)

    def populate_data_diff_field(
        self, item: dict, previous_item: Union[dict, None]
    ) -> Union[dict, None]:
        result = {}
        result["diff"] = {}
        if previous_item is None:
            result["is_new"] = True
            return result
        result["is_new"] = False

        for key, value in copy.deepcopy(item).items():
            previous = previous_item.get(key)
            if previous != value:
                result["diff"][key] = {"previous": previous, "current": value}

        item[self.DATA_DIFF_FIELD] = result

        return item

    def drop_item(self, item: dict) -> bool:
        """
        Decide if we should drop the item based on the info in item[DATA_DIFF_FIELD]
        Default beahviour is to drop the item if it wasn't modified
        """
        return not item[self.DATA_DIFF_FIELD]["diff"]

    def process_item(self, item, spider):
        primary_key = self.get_pk(item)
        previous_item = self._get_previous_item(primary_key)

        item = self.populate_data_diff_field(item, previous_item)

        if self.drop_item(item, previous_item):
            raise DropItem()

        if not self.KEEP_DATA_DIFF_FIELD:
            del item[self.DATA_DIFF_FIELD]
        return item
