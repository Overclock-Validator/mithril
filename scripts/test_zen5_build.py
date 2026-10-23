import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("zen5_build", Path(__file__).with_name("zen5-build.py"))
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


class ArtifactValidation(unittest.TestCase):
    def test_accepts_final_offset(self):
        self.assertEqual(build.interpreter_address(f" 8edae0 14223 t {build.SYMBOL}\n", 32), 0x8edae0)

    def test_rejects_ignored_placement(self):
        with self.assertRaisesRegex(ValueError, "expected mod64=32"):
            build.interpreter_address(f" 8edac0 14223 T {build.SYMBOL}\n", 32)

    def test_rejects_missing_or_duplicate_function(self):
        for nm in ("", f" 8edae0 14223 t {build.SYMBOL}\n 9edae0 14223 T {build.SYMBOL}\n"):
            with self.assertRaisesRegex(ValueError, "expected one"):
                build.interpreter_address(nm, 32)

    def test_rejects_unsupported_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "binary"
            for header in (b"", b"not an executable", b"\x7fELF\x02\x01" + bytes(12) + b"\xb7\x00"):
                path.write_bytes(header)
                with self.assertRaisesRegex(ValueError, "Linux/amd64"):
                    build.check_elf(path)


if __name__ == "__main__":
    unittest.main()
