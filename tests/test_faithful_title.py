from routing import assemble_faithful_title, derive_title, plan_message


def test_keeps_action_and_object_span_without_truncation():
    assert assemble_faithful_title("危废联单", "录入联单信息", max_length=80) == "危废联单：录入联单信息"


def test_long_or_missing_span_fails_closed():
    assert assemble_faithful_title("巡库手册", "汇总意见建议" * 20, max_length=24) is None
    assert assemble_faithful_title("巡库手册", None) is None


def test_does_not_invent_action_or_deadline():
    assert assemble_faithful_title("库存盘点", "核对库存") == "库存盘点：核对库存"


def test_short_notice_title_preserves_object_and_all_source_requirements():
    source = "通知：网络安全承诺书科室及工班人员是否已签订完毕。@所有人 今天上午11点前完成反馈。"
    from datetime import date
    plan = plan_message({"_jev_v2_applicable": True, "text": source, "group": "工班"}, {}, {"name": "李林骁"}, run_date=date(2026, 10, 7))
    title = plan["title"]
    assert "网络安全承诺书" in title and "11点前完成反馈" in title
