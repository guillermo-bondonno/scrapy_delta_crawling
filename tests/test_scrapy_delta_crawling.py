#!/usr/bin/env python

"""Tests for `scrapy_delta_crawling` package."""


import unittest
from dotenv import load_dotenv
from scrapy_delta_crawling import scrapy_delta_crawling

load_dotenv()


class TestScrapy_delta_crawling(unittest.TestCase):
    """Tests for `scrapy_delta_crawling` package."""

    def setUp(self):
        scrapy_delta_crawling.DeltaCrawlingPipeline
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_something(self):
        """Test something."""
