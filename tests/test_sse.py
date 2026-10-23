import unittest
from monitor_portal_server import SSEBroker
class SSETests(unittest.TestCase):
    def test_fanout_and_disconnect(self):
        broker=SSEBroker()
        first, second=broker.add_client(), broker.add_client()
        broker.broadcast('log', {'message':'synthetic'})
        self.assertEqual(first.get_nowait(),second.get_nowait())
        broker.remove_client(first)
        broker.broadcast('log', {'message':'next'})
        self.assertTrue(first.empty())
        self.assertIn('next',second.get_nowait())
    def test_full_client_does_not_block(self):
        broker=SSEBroker()
        full, healthy=broker.add_client(),broker.add_client()
        for _ in range(full.maxsize): full.put_nowait('old')
        broker.broadcast('log', {'message':'new'})
        self.assertNotIn(full,broker.clients)
        self.assertIn('new',healthy.get_nowait())
if __name__=='__main__': unittest.main()
