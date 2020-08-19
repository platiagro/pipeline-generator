from unittest import TestCase

from pipelines.api import app


class TestLogger(TestCase):

    def test_logger(self):
        with app.test_client() as c:
            rv = c.post("/seldon/logger/cdf47789-934d-4efa-a412-0bfacf9a466a", json={

                    "random": 21,
                    "random float": 14.448,
                    "bool": False,
                    "date": "1987-02-25",
                    "regEx": "hellooooooooooooooooooooooooooooooooooo world",
                    "enum": "online",
                    "firstname": "Patricia",
                    "lastname": "Ehrman",
                    "city": "Peshawar",
                    "country": "Fiji",
                    "countryCode": "TH",
                    "email uses current data": "Patricia.Ehrman@gmail.com",
                    "email from expression": "Patricia.Ehrman@yopmail.com",
                    "array": [
                        "Meghann",
                        "Jerry",
                        "Annaliese",
                        "Merry",
                        "Jaclyn"
                    ],
                    "array of objects": [
                        {
                            "index": 0,
                            "index start at 5": 5
                        },
                        {
                            "index": 1,
                            "index start at 5": 6
                        },
                        {
                            "index": 2,
                            "index start at 5": 7
                        }
                    ]

            })
            result = rv.get_data(as_text=True)
            #expected = "{\"message\":\"PlatIAgro Pipelines v0.0.1\"}\n"
            #self.assertEqual(result, expected)
            self.assertEqual(rv.status_code, 200)
