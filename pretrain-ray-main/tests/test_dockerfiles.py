from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DockerfileTests(unittest.TestCase):
    def test_controller_wheel_build_does_not_resolve_dependencies(self) -> None:
        dockerfile = (ROOT / "docker" / "Dockerfile.controller").read_text()
        wheel_step = dockerfile.split("python -m pip wheel", 1)[1].split(".", 1)[0]
        self.assertIn("--no-deps", wheel_step)

    def test_head_redeclares_kubectl_source_in_build_stage(self) -> None:
        dockerfile = (ROOT / "docker" / "Dockerfile.head").read_text()
        final_stage = dockerfile.split("FROM ${BASE_IMAGE}", 1)[1]
        self.assertIn("ARG KUBECTL_SOURCE=/bin/kubectl", final_stage)
        self.assertIn(
            "COPY --from=kubectl ${KUBECTL_SOURCE} /usr/local/bin/kubectl",
            final_stage,
        )

    def test_cluster_worker_build_preserves_ray_runtime_bin(self) -> None:
        script = (ROOT / "scripts" / "build-worker-image-on-cluster.sh").read_text()
        self.assertIn("runtime_bin=${KCC_WORKER_RUNTIME_BIN:-}", script)
        self.assertIn("runtime_bin=${resolved_worker_python%/*}", script)
        self.assertIn("'test -x \"$1/ray\"'", script)
        self.assertIn('grep -Fq "PATH=$runtime_bin:"', script)


if __name__ == "__main__":
    unittest.main()
