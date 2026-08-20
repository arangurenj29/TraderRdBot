import os
import unittest
from unittest.mock import patch

from traderrd.config import ConfigurationError, load_config


BASE_ENVIRONMENT = {
    "TELEGRAM_API_ID": "12345",
    "TELEGRAM_API_HASH": "local-test-placeholder",
    "TELEGRAM_SOURCE_CHAT_ID": "2180632014",
    "TELEGRAM_SOURCE_TOPIC_ID": "231508",
    "TELEGRAM_SOURCE_SEED_MESSAGE_ID": "231906",
}


class ObserverConfigTests(unittest.TestCase):
    @patch.dict(os.environ, BASE_ENVIRONMENT, clear=True)
    def test_uses_safe_public_bybit_defaults(self) -> None:
        config = load_config("/tmp/traderrd-config-test-does-not-exist.env")
        self.assertEqual(config.bybit_public_base_url, "https://api.bybit.com")
        self.assertEqual(config.bybit_category, "linear")
        self.assertEqual(config.bybit_timeout_seconds, 3.0)
        self.assertEqual(config.source_topic_id, 231508)
        self.assertEqual(config.source_seed_message_id, 231906)
        self.assertIsNone(config.expected_sender_id)

    @patch.dict(
        os.environ,
        {**BASE_ENVIRONMENT, "BYBIT_CATEGORY": "spot"},
        clear=True,
    )
    def test_rejects_non_linear_bybit_category(self) -> None:
        with self.assertRaises(ConfigurationError) as raised:
            load_config("/tmp/traderrd-config-test-does-not-exist.env")
        self.assertIn("must be linear", str(raised.exception))

    @patch.dict(
        os.environ,
        {**BASE_ENVIRONMENT, "TELEGRAM_EXPECTED_SENDER_ID": "-1001234567890"},
        clear=True,
    )
    def test_accepts_numeric_expected_sender_as_optional_second_guard(self) -> None:
        config = load_config("/tmp/traderrd-config-test-does-not-exist.env")
        self.assertEqual(config.expected_sender_id, -1001234567890)

    @patch.dict(
        os.environ,
        {
            key: value
            for key, value in BASE_ENVIRONMENT.items()
            if key != "TELEGRAM_SOURCE_TOPIC_ID"
        },
        clear=True,
    )
    def test_rejects_missing_topic_configuration(self) -> None:
        with self.assertRaises(ConfigurationError) as raised:
            load_config("/tmp/traderrd-config-test-does-not-exist.env")
        self.assertIn("TELEGRAM_SOURCE_TOPIC_ID", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
