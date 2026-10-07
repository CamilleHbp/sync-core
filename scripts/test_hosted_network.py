import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('hosted_network', Path(__file__).with_name('hosted-network.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class HostedPorts(unittest.TestCase):
    def test_only_authenticated_sync_ports_are_published(self):
        names = ['any-sync-'+name for name in module.SERVICES] + ['mongo-1','redis','garage','netcheck','any-sync-init']
        config = {'services': {name: {'ports':[{'published':'9999','target':9999}]} for name in names}}
        actual = module.constrain(config)
        for name, service in actual['services'].items():
            if name in ['any-sync-'+n for n in module.SERVICES]:
                self.assertEqual(len(service['ports']),2)
                self.assertEqual([p['protocol'] for p in service['ports']],['tcp','udp'])
                self.assertTrue(all(p['host_ip']=='0.0.0.0' for p in service['ports']))
            else:
                self.assertNotIn('ports',service)
    def test_ports_are_stable_and_unique(self):
        config={'services':{'any-sync-'+name:{} for name in module.SERVICES}}
        result=module.constrain(config)
        ports=[int(p['published']) for s in result['services'].values() for p in s['ports']]
        self.assertEqual(sorted(ports),list(range(19001,19007))+list(range(19101,19107)))

if __name__ == '__main__': unittest.main()
