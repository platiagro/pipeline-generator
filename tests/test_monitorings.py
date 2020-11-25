# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app
from pipelines.database import engine
from pipelines.utils import uuid_alpha

EXPERIMENT_ID = str(uuid_alpha())
PROJECT_ID = str(uuid_alpha())
MONITORING_ID = str(uuid_alpha())
NAME = "foo"
POSITION = 0
CREATED_AT = "2000-01-01 00:00:00"
CREATED_AT_ISO = "2000-01-01T00:00:00"
UPDATED_AT = "2000-01-01 00:00:00"
UPDATED_AT_ISO = "2000-01-01T00:00:00"


class TestMonitorings(TestCase):
    def setUp(self):
        self.maxDiff = None
        conn = engine.connect()
        text = (
            f"INSERT INTO projects (uuid, name, created_at, updated_at) "
            f"VALUES ('{PROJECT_ID}', '{NAME}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EXPERIMENT_ID}', '{NAME}', '{PROJECT_ID}', '{POSITION}', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO monitoring (uuid, project_id, created_at, updated_at) "
            f"VALUES ('{MONITORING_ID}', '{PROJECT_ID}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        conn.close()

    def tearDown(self):
        conn = engine.connect()

        text = f"DELETE FROM monitoring WHERE project_id in ('{PROJECT_ID}')"
        conn.execute(text)

        text = f"DELETE FROM experiments WHERE project_id in ('{PROJECT_ID}')"
        conn.execute(text)

        text = f"DELETE FROM projects WHERE uuid = '{PROJECT_ID}'"
        conn.execute(text)

        conn.close()

    def test_list_monitorings(self):
        with app.test_client() as c:
            rv = c.get("/projects/unk/monitorings")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/{PROJECT_ID}/monitorings")
            result = rv.get_json()
            self.assertIsInstance(result, list)

    def test_create_monitoring(self):
        with app.test_client() as c:
            rv = c.post("/projects/unk/monitorings")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.post(f"/projects/{PROJECT_ID}/monitorings")
            result = rv.get_json()
            expected = {
                "projectId": PROJECT_ID,
                "experimentId": None,
                "operatorId": None,
                "runId": None,
                "layout": None,
            }
            machine_generated = ["uuid", "createdAt", "updatedAt"]
            for attr in machine_generated:
                self.assertIn(attr, result)
                del result[attr]
            self.assertDictEqual(expected, result)

    def test_update_monitoring(self):
        with app.test_client() as c:
            rv = c.patch(f"/projects/foo/monitorings/{MONITORING_ID}", json={})
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/monitorings/foo", json={})
            result = rv.get_json()
            expected = {"message": "The specified monitoring does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/monitorings/{MONITORING_ID}", json={
                "experimentId": "unk",
            })
            result = rv.get_json()
            expected = {"message": "The specified experiment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/monitorings/{MONITORING_ID}", json={
                "unk": "bar",
            })
            self.assertEqual(rv.status_code, 400)

            rv = c.patch(f"/projects/{PROJECT_ID}/monitorings/{MONITORING_ID}", json={
                "experimentId": EXPERIMENT_ID,
            })
            result = rv.get_json()
            expected = {
                "uuid": MONITORING_ID,
                "projectId": PROJECT_ID,
                "experimentId": EXPERIMENT_ID,
                "operatorId": None,
                "runId": None,
                "layout": None,
            }
            machine_generated = ["createdAt", "updatedAt"]
            for attr in machine_generated:
                self.assertIn(attr, result)
                del result[attr]
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

    def test_delete_monitoring(self):
        with app.test_client() as c:
            rv = c.delete(f"/projects/foo/monitorings/{MONITORING_ID}")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.delete(f"/projects/{PROJECT_ID}/monitorings/unk")
            result = rv.get_json()
            expected = {"message": "The specified monitoring does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.delete(f"/projects/{PROJECT_ID}/monitorings/{MONITORING_ID}")
            result = rv.get_json()
            expected = {"message": "Monitoring deleted"}
            self.assertDictEqual(expected, result)
