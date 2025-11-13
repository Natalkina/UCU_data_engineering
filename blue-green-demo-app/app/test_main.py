import unittest
from main import app


class TestApp(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_root_endpoint(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data.decode(), "Нехай цей день буде добрим")

if __name__ == '__main__':
    unittest.main()
