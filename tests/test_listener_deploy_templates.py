from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_daily_recovery_timer_is_one_1730_shanghai_calendar_fire_without_periodic_polling():
    timer = (ROOT / "deploy" / "dws-history-recovery.timer.template").read_text()
    assert "OnCalendar=*-*-* 17:30:00 Asia/Shanghai" in timer
    assert "OnUnitActiveSec" not in timer
    assert "OnBootSec" not in timer
    assert "20min" not in timer
    assert "20m" not in timer


def test_service_templates_keep_explicit_write_gates_and_graceful_stop():
    listener = (ROOT / "deploy" / "dws-history-listener.service.template").read_text()
    recovery = (ROOT / "deploy" / "dws-history-recovery.service.template").read_text()
    for unit in (listener, recovery):
        assert "--mode production --write-ledger --allow-external-writes" in unit
        assert "@CURRENT_MODEL_FLAGS@" in unit
        assert "@CURRENT_NOT_BEFORE_ARGS@" in unit
        assert "SendSIGKILL=no" in unit
    assert "--recover-once" in recovery
