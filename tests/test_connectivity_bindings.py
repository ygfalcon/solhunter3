import logging


from solhunter_zero.production.connectivity import ConnectivityChecker


def test_ui_binding_warns_on_remote_localhost_default(caplog):
    caplog.set_level(logging.WARNING)

    checker = ConnectivityChecker(env={"CONNECTIVITY_ASSUME_REMOTE_UI": "1"})

    ui_http_targets = [t for t in checker.targets if t.get("name") == "ui-http"]
    assert ui_http_targets, "expected UI HTTP target to be derived"

    assert any(
        "remote UI access is expected" in record.message for record in caplog.records
    ), caplog.text


def test_ui_binding_warns_on_config_mismatch(caplog):
    caplog.set_level(logging.WARNING)

    checker = ConnectivityChecker(
        env={
            "UI_HOST": "0.0.0.0",
            "UI_PORT": "7000",
            "CONNECTIVITY_ASSUME_LOCAL_UI": "1",
        }
    )

    ui_http_targets = [t for t in checker.targets if t.get("name") == "ui-http"]
    assert ui_http_targets, "expected UI HTTP target to be derived"

    assert any(
        "UI target normalized" in record.message and "config UI_HOST=0.0.0.0" in record.message
        for record in caplog.records
    ), caplog.text
