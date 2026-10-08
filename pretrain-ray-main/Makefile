.PHONY: test validate compile helm-lint package audit check clean

PYTHON ?= python3
HELM ?= helm
CHART := deploy/helm/kcc-training-stable
EXAMPLES := examples/runtime-profile.yaml examples/recipe.yaml examples/training-run.yaml

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

validate:
	@for document in $(EXAMPLES); do \
		PYTHONPATH=src $(PYTHON) -m kcc_training.cli validate "$$document"; \
	done
	@for schema in contracts/*.schema.json; do \
		$(PYTHON) -m json.tool "$$schema" >/dev/null; \
	done

compile:
	$(PYTHON) -m compileall -q src tests ray_startup_bundle

helm-lint:
	$(HELM) lint $(CHART)
	$(HELM) template kcc-training $(CHART) --namespace kcc-training >/dev/null

package:
	$(PYTHON) -m pip wheel --no-deps --no-build-isolation -w dist .

audit:
	bash scripts/stable-audit.sh

check: audit

clean:
	@printf '%s\n' 'Remove local build outputs explicitly when they are no longer needed.'

