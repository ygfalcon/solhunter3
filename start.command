#!/bin/zsh
UI_PORT=5001 PYTORCH_ENABLE_MPS_FALLBACK=1 python -m scripts.startup --foreground --ui-port ${UI_PORT}
