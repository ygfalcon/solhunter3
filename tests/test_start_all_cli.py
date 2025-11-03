import os

from scripts import start_all


def test_parse_args_supports_runtime_flags(tmp_path):
    token_list = tmp_path / "tokens.txt"
    token_list.write_text("SOL\n")
    weight_one = tmp_path / "w1.toml"
    weight_two = tmp_path / "w2.toml"
    for path in (weight_one, weight_two):
        path.write_text("{}")

    args = start_all.parse_args(
        [
            "--dry-run",
            "--offline",
            "--testnet",
            "--token-list",
            str(token_list),
            "--strategy-rotation-interval",
            "7",
            "--weight-config",
            str(weight_one),
            "--weight-config",
            str(weight_two),
        ]
    )

    assert args.dry_run is True
    assert args.offline is True
    assert args.testnet is True
    assert args.token_list == str(token_list)
    assert args.strategy_rotation_interval == 7
    assert args.weight_configs == [str(weight_one), str(weight_two)]


def test_runtime_env_from_argv_normalizes_values(tmp_path):
    token_list = tmp_path / "tokens.txt"
    token_list.write_text("")
    weight_one = tmp_path / "w1.toml"
    weight_two = tmp_path / "w2.toml"
    weight_one.write_text("{}")
    weight_two.write_text("{}")

    env = start_all.runtime_env_from_argv(
        [
            "--dry-run",
            "--offline",
            "--testnet",
            "--token-list",
            str(token_list),
            "--strategy-rotation-interval",
            "13",
            "--weight-config",
            str(weight_one),
            "--weight-config",
            str(weight_two),
        ]
    )

    assert env["DRY_RUN"] == "1"
    assert env["SOLHUNTER_OFFLINE"] == "1"
    assert env["SOLHUNTER_TESTNET"] == "1"
    assert env["TESTNET"] == "1"
    assert env["TOKEN_LIST"] == str(token_list)
    assert env["TOKEN_FILE"] == str(token_list)
    assert env["STRATEGY_ROTATION_INTERVAL"] == "13"

    expected_weights = os.pathsep.join([str(weight_one), str(weight_two)])
    assert env["WEIGHT_CONFIGS"] == expected_weights
    assert env["WEIGHT_CONFIG_PATHS"] == expected_weights
