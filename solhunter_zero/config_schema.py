from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel, AnyUrl, ValidationError

try:  # Pydantic v2
    from pydantic import field_validator, model_validator, ValidationInfo  # type: ignore
except Exception:  # pragma: no cover - fallback for Pydantic v1
    from pydantic import validator as field_validator, root_validator
    model_validator = None  # type: ignore
    ValidationInfo = None  # type: ignore


class ConfigModel(BaseModel):
    """Schema for SolHunter configuration files."""

    solana_rpc_url: AnyUrl
    dex_base_url: AnyUrl
    agents: List[str]
    agent_weights: Dict[str, float]

    class Config:
        extra = "allow"

    @field_validator("agents")
    def _agents_non_empty(cls, value: List[str]) -> List[str]:
        if not value or not all(isinstance(a, str) and a.strip() for a in value):
            raise ValueError("agents must be a list of non-empty strings")
        return value

    if model_validator is not None and ValidationInfo is not None:  # Pydantic v2

        @field_validator("agent_weights")
        def _weights_for_agents(
            cls, value: Dict[str, float], info: ValidationInfo
        ) -> Dict[str, float]:
            agents = []
            if getattr(info, "data", None):
                agents = list(info.data.get("agents") or [])
            missing = [a for a in agents if a not in value]
            if missing:
                raise ValueError(
                    f"missing weight for agent(s): {', '.join(missing)}"
                )
            return value

    else:  # pragma: no cover - Pydantic v1

        @root_validator
        def _weights_for_agents(cls, values: Dict[str, object]) -> Dict[str, object]:
            agents = values.get("agents") or []
            weights = values.get("agent_weights") or {}
            missing = [a for a in agents if a not in weights]
            if missing:
                raise ValueError(
                    f"missing weight for agent(s): {', '.join(missing)}"
                )
            return values


def validate_config(data: Dict[str, object]) -> Dict[str, object]:
    """Validate ``data`` against :class:`ConfigModel`.

    Returns the validated data with type normalization applied.
    Raises ``ValueError`` on validation errors.
    """
    try:
        model = ConfigModel(**data)
        if hasattr(model, "model_dump"):
            return model.model_dump()
        return model.dict()  # type: ignore[return-value]
    except ValidationError as exc:  # pragma: no cover - pass through as ValueError
        raise ValueError(str(exc)) from exc
