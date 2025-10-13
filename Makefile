# Simple Makefile to run SolHunter Zero
# The `run` target uses the cross-platform Python entry point.

PYTHON ?= python3

.RECIPEPREFIX := >

.PHONY: start run test integration demo demo-rl demo-multi setup gen-proto compose

start:
>$(PYTHON) -m solhunter_zero.launcher $(ARGS)

setup:
>$(PYTHON) -m solhunter_zero.launcher --one-click $(ARGS)

# Launch directly without the shell script (works on all platforms)
run:
>$(PYTHON) -m solhunter_zero.main --auto $(ARGS)

test:
>$(PYTHON) -m pytest $(ARGS)

integration:
>$(PYTHON) -m pytest -m integration $(ARGS)

typecheck:
>$(PYTHON) -m mypy solhunter_zero tests

gen-proto:
>$(PYTHON) scripts/gen_proto.py

audit-placeholders:
>scripts/audit_placeholders.sh $(ARGS)

# Run the investor demo. Pass ARGS to override or extend defaults.
# Examples:
#   make demo ARGS="--preset multi"
#   make demo ARGS="--rl-demo --reports reports"
demo:
>$(PYTHON) demo.py --preset short --reports reports $(ARGS)

demo-multi:
>$(MAKE) demo ARGS="--preset multi"

demo-rl:
>$(MAKE) demo ARGS="--rl-demo --reports reports"

paper:
>$(PYTHON) paper.py --reports reports $(ARGS)

paper-test:
>SOLHUNTER_TESTING=1 $(PYTHON) scripts/paper_test.py $(ARGS)

# Prepare environment and run docker-compose
compose:
>$(PYTHON) scripts/prepare_env.py
>docker-compose up $(ARGS)
devnet-dryrun:
	@bash scripts/devnet_dryrun.sh $(ARGS)

