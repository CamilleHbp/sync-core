"""Unit tests for the private deployment wrapper; no Docker/network required."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("private_network", Path(__file__).with_name("private-network.py"))
network = importlib.util.module_from_spec(spec)
spec.loader.exec_module(network)


class PrivateNetworkTest(unittest.TestCase):
    def test_port_check_rejects_wildcard_and_default_bindings(self):
        for port in ({"published": "19001"}, {"host_ip": "0.0.0.0"}, {"host_ip": "::"}):
            config = json.dumps({"services": {"node": {"ports": [port]}}}).encode()
            with patch.object(network.subprocess, "check_output", return_value=config):
                with self.assertRaisesRegex(ValueError, "non-loopback"):
                    network.check_ports(Path("/unused"))

    def test_port_check_accepts_loopback_and_unpublished_services(self):
        config = json.dumps({"services": {"node": {"ports": [{"host_ip": "127.0.0.1"}]}, "database": {}}}).encode()
        with patch.object(network.subprocess, "check_output", return_value=config):
            network.check_ports(Path("/unused"))

    def test_override_replaces_all_published_ports(self):
        text = network.override()
        self.assertEqual(text.count("ports: !override"), 8)
        self.assertEqual(text.count('"127.0.0.1:'), 12)
        self.assertNotIn("0.0.0.0", text)

    def test_secret_file_cannot_overwrite_existing_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "generated"
            network.private_write(path, "synthetic")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                network.private_write(path, "replacement")
            self.assertEqual(path.read_text(), "synthetic")

    def test_missing_or_duplicated_upstream_setting_fails_closed(self):
        for template in ("", "EXTERNAL_LISTEN_HOSTS=x\nEXTERNAL_LISTEN_HOSTS=y\n"):
            with self.assertRaisesRegex(ValueError, "exactly one upstream setting"):
                network.environment(template)

    def test_prepare_refuses_existing_directory_without_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("sys.argv", ["private-network.py", "prepare", "--directory", directory]), patch.object(network, "run") as run:
                with self.assertRaises(FileExistsError):
                    network.main()
                run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
