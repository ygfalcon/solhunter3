from __future__ import annotations

import pickle
import random
from argparse import ArgumentParser
from typing import Iterable, Tuple, List
import asyncio

from .http import close_session
from .util import install_uvloop

install_uvloop()

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    LargeBinary,
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class Experience(Base):
    __tablename__ = "replay"

    id = Column(Integer, primary_key=True)
    state = Column(LargeBinary, nullable=False)
    action = Column(String, nullable=False)
    reward = Column(Float, nullable=False)
    emotion = Column(String)
    regime = Column(String)


class ReplayBuffer:
    """Persistent replay buffer backed by SQLite."""

    def __init__(self, url: str = "sqlite:///replay.db") -> None:
        self.engine = create_engine(url, echo=False, future=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    # ------------------------------------------------------------------
    def add(
        self,
        state: Iterable[float],
        action: str,
        reward: float,
        emotion: str = "",
        regime: str = "",
    ) -> None:
        data = pickle.dumps(list(state))
        with self.Session() as session:
            exp = Experience(
                state=data,
                action=action,
                reward=reward,
                emotion=emotion,
                regime=regime,
            )
            session.add(exp)
            session.commit()

    # ------------------------------------------------------------------
    def sample(
        self,
        batch_size: int,
        *,
        positive_weight: float = 2.0,
        regime: str | None = None,
    ) -> List[Tuple[List[float], str, float, str, str]]:
        with self.Session() as session:
            q = session.query(Experience)
            if regime is not None:
                q = q.filter_by(regime=regime)
            exps: List[Experience] = q.all()
        filtered = [e for e in exps if e.emotion != "regret"]
        if not filtered:
            return []
        weights = [positive_weight if e.emotion == "confident" else 1.0 for e in filtered]
        total = float(sum(weights))
        probs = [w / total for w in weights]
        indices = list(range(len(filtered)))
        chosen = random.choices(indices, weights=probs, k=min(batch_size, len(filtered)))
        result = []
        for idx in chosen:
            e = filtered[idx]
            state = pickle.loads(e.state)
            result.append((state, e.action, float(e.reward), e.emotion, e.regime or ""))
        return result

    # ------------------------------------------------------------------
    def list_entries(self) -> List[Experience]:
        with self.Session() as session:
            return session.query(Experience).all()

    def __len__(self) -> int:  # pragma: no cover - trivial
        with self.Session() as session:
            return session.query(Experience).count()


# ----------------------------------------------------------------------

def main(argv: List[str] | None = None) -> int:
    parser = ArgumentParser(description="Inspect replay buffer")
    parser.add_argument("--db", default="sqlite:///replay.db", help="Database URL")
    args = parser.parse_args(argv)
    buf = ReplayBuffer(args.db)
    for e in buf.list_entries():
        state = pickle.loads(e.state)
        print(
            f"{e.id}\t{state}\t{e.action}\t{e.reward}\t{e.emotion}\t{e.regime}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    finally:
        asyncio.run(close_session())
