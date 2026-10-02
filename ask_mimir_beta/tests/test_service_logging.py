import logging
import logging.config
import unittest
from unittest.mock import patch

from serve import service_log_config


class ServiceLoggingTests(unittest.TestCase):
    def test_production_config_emits_one_info_lifecycle_record(self):
        logging.config.dictConfig(service_log_config())
        logger = logging.getLogger('ask-mimir')
        with patch.object(logger.handlers[0], 'emit') as emit:
            logger.info('ask_server_completed request=test')
        self.assertEqual(emit.call_count, 1)
        self.assertEqual(emit.call_args.args[0].getMessage(), 'ask_server_completed request=test')
        self.assertFalse(logger.propagate)
