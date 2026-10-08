from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ray_startup_bundle"))

import start_ray  # noqa: E402


class LegacyWorkspaceConfigTests(unittest.TestCase):
    def test_start_forwards_workspace_and_image_overrides_to_renderer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            args = start_ray.make_parser().parse_args(
                [
                    "--node",
                    "worker-a",
                    "--workspace-host-path",
                    "/srv/kcc-workspace",
                    "--ray-head-image",
                    "registry.example/ray/head:test",
                    "--ray-worker-image",
                    "registry.example/ray/worker:a3-arm",
                    "--ray-image-pull-policy",
                    "Never",
                    "--training-artifact-root",
                    temporary_directory,
                ]
            )

            stages = start_ray.build_stage_commands(args, run_id="workspace-wire")
            render_command = list(stages[1][1])
            option_index = render_command.index("--workspace-host-path")
            self.assertEqual(
                render_command[option_index + 1],
                "/srv/kcc-workspace",
            )
            expected_values = {
                "--ray-head-image": "registry.example/ray/head:test",
                "--ray-worker-image": "registry.example/ray/worker:a3-arm",
                "--ray-image-pull-policy": "Never",
            }
            for option, expected in expected_values.items():
                option_index = render_command.index(option)
                self.assertEqual(render_command[option_index + 1], expected)


if __name__ == "__main__":
    unittest.main()
