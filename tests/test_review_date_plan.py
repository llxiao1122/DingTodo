from review_date_plan import source_deadline


def test_explicit_relative_deadline_uses_source_message_date():
    assert source_deadline('请明天15:00前更新台账', '2026-11-05 09:00:00') == '2026-11-06T15:00:00+08:00'


def test_ambiguous_or_unrelated_dates_do_not_become_deadlines():
    cases = [
        '请明天下班前更新台账',
        '请周五15:00前更新台账',
        '明天15:00召开会议',
        '请明天15:00前提交，后天16:00前复核',
        '请明天15:00前提交，未完成将纳入考核',
        '请明天15:00前更新，次日16:00检查',
        '请明天15:00前更新，2026年2月30日前补录',
        '请明天前完成任务。其他事项18:00开会',
        '请明天前完成任务，18:00开会',
        '请明天前完成非常重要的配电设备安全检查并书面确认反馈领导18:00开会',
    ]
    for text in cases:
        assert source_deadline(text, '2026-11-05 09:00:00') is None


def test_message_date_and_explicit_clock_are_required():
    assert source_deadline('请明天15:00前更新台账', '') is None
    assert source_deadline('请明天前更新台账', '2026-11-05 09:00:00') is None
    assert source_deadline('请明天15:00前更新台账', '2026-11-05T18:00:00Z') == '2026-11-07T15:00:00+08:00'
