import argparse
import asyncio
from .rl_daemon import RLDaemon, parameter_server
from .http import close_session
from .util import install_uvloop
from .device import get_default_device

install_uvloop()

async def main() -> None:
    p = argparse.ArgumentParser(description="Run RL training daemon")
    p.add_argument("--memory", default="sqlite:///memory.db")
    p.add_argument("--data", default="offline_data.db")
    p.add_argument("--model", default="ppo_model.pt")
    p.add_argument("--algo", default="ppo", choices=["ppo", "dqn", "a3c", "ddpg"]) 
    p.add_argument("--policy", default="mlp", choices=["mlp", "transformer"], help="Policy network type")
    p.add_argument("--interval", type=float, default=3600.0)
    p.add_argument("--num-workers", type=int, default=None, help="Data loader workers")
    p.add_argument("--device", default="auto")
    p.add_argument("--daemon", action="store_true", help="Run in background")
    p.add_argument("--distributed", action="store_true", help="Enable distributed training")
    p.add_argument("--parameter-server", action="store_true", help="Run parameter server")
    args = p.parse_args()

    device = get_default_device(args.device)

    if args.parameter_server:
        sub = parameter_server()
        try:
            await asyncio.Event().wait()
        finally:
            sub.__exit__(None, None, None)
        return

    if args.num_workers is not None:
        import os
        os.environ["RL_NUM_WORKERS"] = str(args.num_workers)
    daemon = RLDaemon(
        memory_path=args.memory,
        data_path=args.data,
        model_path=args.model,
        algo=args.algo,
        policy=args.policy,
        device=device,
        distributed_rl=args.distributed,
    )
    daemon.start(args.interval)
    if args.daemon:
        try:
            while True:
                await asyncio.sleep(3600)
        except KeyboardInterrupt:
            pass
    else:
        await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        asyncio.run(close_session())
