# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.database import init_db


class TestDatabase(TestCase):

    def test_init_db(self):
        init_db()
