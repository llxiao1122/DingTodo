"""Deterministic responsibility routing for the Iron West workgroup.

The module is deliberately independent of dws, PolarDB, and any LLM.  It turns
one normalized source message into a CREATE/REVIEW/IGNORE plan.  A writer may
consume a CREATE plan only after resolving live DingTalk IDs and performing
remote read-back.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
import hashlib
import re
from typing import Any, Iterable

from assignment_policy import (
    AssignmentContractError,
    classify_rule,
    load_contract,
    resolve_confirmed_assignment,
)


ROUTER_VERSION = "iron-west-responsibility-v1"
SELF_NAME = "李林骁"
# A process-local capability token. JSON event fields cannot authorize this
# narrow route; EventProcessor issues the token only after local evidence checks.
CONTEXTUAL_FOLLOWUP_VERIFIED_TOKEN = object()

# Ordered from specific responsibility modules to generic ledger/management
# routes.  The order is part of the business rule and is covered by tests.
ROUTES = (
    ("attendance", "陈红洁", ("考勤", "出勤", "打卡", "签到", "请假", "休假")),
    (
        "training",
        "张志斌",
        ("培训", "学习", "宣贯", "继续教育", "考试", "答题", "学时", "培训合格", "文件流转"),
    ),
    ("drill", "陈红洁", ("消防", "灭火", "防火", "演练")),
    # These two routes are bound to the current area-duty matrix at runtime.
    ("hazardous_waste", "杨梦卓", ("危废", "危险废弃物", "危险废物", "危废清单", "危险废物清单", "联单程序", "联单信息", "联单编号")),
    ("scrap", "谭继衡", ("废旧", "废旧物资")),
    ("asset", "杨梦卓", ("资产", "计量", "设备", "仪器", "固定资产")),
    (
        "materials",
        "苗笑天",
        ("物资", "材料", "需求", "电商", "办公用品", "二级库", "二级仓", "库存"),
    ),
    ("leader_inspection", "李林骁", ("工班长", "全局", "专项", "检查通报", "检查安排", "检查验收")),
    ("ledger", "谭继衡", ("台账", "台帐")),
)

ACTION_TERMS = (
    "请",
    "需要",
    "完成",
    "提交",
    "报送",
    "上报",
    "更新",
    "核对",
    "整理",
    "组织",
    "学习",
    "审阅",
    "审核",
    "落实",
    "跟进",
    "处理",
    "推进",
    "开展",
    "参加",
    "收集",
    "验收",
    "发送",
    "反馈",
    "申报",
    "盘点",
    "检查",
    "整改",
    "复核",
)

ACK_TERMS = ("收到", "知道了", "好的", "OK", "已完成", "已处理", "搞定", "完成了")
COMPLETION_PREFIXES = (
    "已完成",
    "已处理",
    "搞定",
    "完成了",
    "已整改",
    "已反馈",
    "已提交",
    "已报送",
    "已核对",
    "已盘点",
    "已落实",
    "已跟进",
)
LOCAL_WORKGROUP_SOURCE_GROUPS = frozenset(
    {
        "物资调配室",
        "危废、废旧工作群",
        "铁炉西工班群",
        "工班长交流群",
    }
)
DEPARTMENT_SCOPE_TERMS = (
    "物资管理中心",
    "物资中心",
    "各科室",
    "各部门",
    "各室",
    "部门各模块",
    "科室各模块",
    "意见征询",
    "意见反馈",
    "反馈意见",
    "分管经理",
)

AREA_ROUTE_REQUIREMENTS = {
    "杂品库": "hazardous_waste",
    "材料棚及办公用房": "scrap",
}
AREA_MATRIX_NAMES = frozenset({"杂品库", "大件库区", "材料棚及办公用房", "中小件区"})


def _reviewed(value: Any) -> bool:
    return value in (1, True, "1", "true", "True")


def _area_date(value: Any) -> date | None:
    text = _as_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def derive_active_routing(
    area_rows: Iterable[dict[str, Any]] | None,
    run_date: date,
) -> tuple[bool, dict[str, str], list[str]]:
    """Derive active cross-routes only from one reviewed row per current area."""
    rows = list(area_rows or [])
    owners: dict[str, str] = {}
    warnings: list[str] = []
    for area in AREA_MATRIX_NAMES:
        candidates = []
        for row in rows:
            if _as_text(row.get("area_name")) != area:
                continue
            start = _area_date(row.get("cycle_start"))
            end = _area_date(row.get("cycle_end"))
            if start and end and start <= run_date <= end:
                candidates.append(row)
        if len(candidates) != 1:
            warnings.append(f"area_route_{area}_not_unique")
            continue
        row = candidates[0]
        if not _reviewed(row.get("reviewed")):
            warnings.append(f"area_route_{area}_not_reviewed")
            continue
        owner = _as_text(row.get("staff_name"))
        if not owner:
            warnings.append(f"area_route_{area}_owner_missing")
            continue
        owners[area] = owner

    if warnings:
        return False, {}, warnings
    rules = {
        route: owners[area]
        for area, route in AREA_ROUTE_REQUIREMENTS.items()
    }
    return True, rules, []


def _as_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def normalize_text(text: str) -> str:
    text = _as_text(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _is_completion_report(text: str) -> bool:
    """Detect a completed-work report before direct-assignment promotion."""
    compact = normalize_text(text)
    compact = re.sub(r"@[^\s]+(?:\([^)]*\))?", "", compact)
    compact = compact.strip(" ：:，,；;")
    if compact.strip("。！! ") in {"无问题", "未发现问题", "排查无问题", "已排查无问题"}:
        return True
    return any(compact.startswith(prefix) for prefix in COMPLETION_PREFIXES)


_CLOCK_COLON_RE = re.compile(
    r"(?P<period>凌晨|早上|上午|中午|下午|傍晚|晚上|今晚)?\s*"
    r"(?P<hour>\d{1,2})[:：](?P<minute>\d{1,2})"
)
_CLOCK_POINT_RE = re.compile(
    r"(?P<period>凌晨|早上|上午|中午|下午|傍晚|晚上|今晚)?\s*"
    r"(?P<hour>[0-9零〇一二两三四五六七八九十百]+)\s*(?:点|时)"
    r"\s*(?P<minute>半|\d{1,2})?\s*分?"
)
_CHINESE_DIGITS = {"零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}


def _chinese_number(value: str) -> int | None:
    value = _as_text(value)
    if not value:
        return None
    if value.isdigit():
        return int(value)
    if value == "十":
        return 10
    if "百" in value:
        head, tail = value.split("百", 1)
        hundreds = _CHINESE_DIGITS.get(head, 0)
        rest = _chinese_number(tail) if tail else 0
        return hundreds * 100 + (rest or 0)
    if "十" in value:
        head, tail = value.split("十", 1)
        tens = _CHINESE_DIGITS.get(head, 1) if head else 1
        ones = _CHINESE_DIGITS.get(tail, 0) if tail else 0
        return tens * 10 + ones
    if len(value) == 1 and value in _CHINESE_DIGITS:
        return _CHINESE_DIGITS[value]
    return None


def _parse_clock_time(text: str) -> time | None:
    text = _as_text(text)
    match = _CLOCK_COLON_RE.search(text) or _CLOCK_POINT_RE.search(text)
    if not match:
        return None
    hour = _chinese_number(match.group("hour"))
    minute_value = match.groupdict().get("minute")
    if hour is None:
        return None
    minute = 30 if minute_value == "半" else (_chinese_number(minute_value) if minute_value else 0)
    if minute is None or minute > 59:
        return None
    period = match.groupdict().get("period") or ""
    if period in {"下午", "傍晚", "晚上", "今晚"} and 1 <= hour < 12:
        hour += 12
    elif period in {"凌晨", "早上", "上午"} and hour == 12:
        hour = 0
    elif not period and 1 <= hour <= 7:
        # In a work notice, "五点前" conventionally means 17:00.
        hour += 12
    if hour > 23:
        return None
    return time(hour, minute)


def _parse_due_day(text: str, run_date: date) -> date | None:
    text = _as_text(text)
    if re.search(r"后天", text):
        return run_date + timedelta(days=2)
    if re.search(r"明天", text):
        return run_date + timedelta(days=1)
    if re.search(r"今天|今日|当天|本日", text):
        return run_date

    iso = re.search(r"(?<!\d)(20\d{2})[-年](\d{1,2})[-月](\d{1,2})日?", text)
    if iso:
        try:
            return date(int(iso.group(1)), int(iso.group(2)), int(iso.group(3)))
        except ValueError:
            return None

    chinese = re.search(r"(?<!\d)(\d{1,2})月(\d{1,2})日", text)
    if chinese:
        try:
            return date(run_date.year, int(chinese.group(1)), int(chinese.group(2)))
        except ValueError:
            return None

    if _parse_clock_time(text) is not None or "下班前" in text:
        return run_date
    return None


def parse_due_datetime(text: str, run_date: date) -> tuple[str, str] | tuple[None, None]:
    """Parse Chinese relative dates and colloquial workday clock deadlines."""
    day = _parse_due_day(text, run_date)
    if day is None:
        return None, None
    clock = _parse_clock_time(text) or time(18, 0)
    due_iso = datetime.combine(day, clock).strftime("%Y-%m-%dT%H:%M:%S+08:00")
    return day.isoformat(), due_iso


def parse_due_date(text: str, run_date: date) -> str | None:
    """Extract an explicit enough date without using an LLM."""
    day, _ = parse_due_datetime(text, run_date)
    return day


_DATE_CANDIDATE_RE = re.compile(
    r"20\d{2}[-年]\d{1,2}[-月]\d{1,2}日?"
    r"|(?<!\d)\d{1,2}月\d{1,2}日"
    r"|后天|明天|今天|今日|当天|本日"
)
_EXPLICIT_DATE_CANDIDATE_RE = re.compile(
    r"^(?:20\d{2}[-年]\d{1,2}[-月]\d{1,2}日?|\d{1,2}月\d{1,2}日)$"
)
_SCHEDULED_EVENT_RE = re.compile(
    r"^\s*(?:将\s*)?"
    r"(?:(?:对|就)[^，,。；;！？\n]{1,16}?\s*)?"
    r"(?:进行|开展|召开|举行|组织|安排|实施)\s*"
    r"[^，,。；;！？\n]{0,12}?"
    r"(?:核查|检查|审计|巡查|验收|盘点|审阅|复核|会议|培训|演练|调研|走访)"
)


def has_competing_assessment_cutoff(text: str) -> bool:
    """Find a clock time tied to unfinished work and an assessment consequence."""
    source = _as_text(text)
    for pattern in (_CLOCK_COLON_RE, _CLOCK_POINT_RE):
        for match in pattern.finditer(source):
            following = source[match.end():match.end() + 100]
            unfinished = re.search(r"(?:仍未|尚未|未)(?:完成|更新|反馈|提交|录入|补录)", following)
            assessed = re.search(
                r"(?:纳入|列入|计入).{0,24}(?:考核|考评)|(?:考核|考评).{0,24}(?:扣分|处罚|追责)",
                following,
            )
            if unfinished and assessed:
                return True
    return False


def extract_due_candidates(text: str, run_date: date, *, include_context: bool = False) -> list[dict[str, str]]:
    """Return only source-derived, Gregorian-validated candidate deadlines."""
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, str]] = []
    raw_source = "" if text is None else str(text)
    leading_whitespace = len(raw_source) - len(raw_source.lstrip())
    source = raw_source.strip()
    segment_spans = []
    segment_start = 0
    for delimiter in re.finditer(r"[；;。！？\n]", source):
        segment_spans.append((segment_start, delimiter.start(), source[segment_start:delimiter.start()]))
        segment_start = delimiter.end()
    segment_spans.append((segment_start, len(source), source[segment_start:]))
    for segment_start, _, segment in segment_spans:
        dates = list(_DATE_CANDIDATE_RE.finditer(segment))
        clocks: dict[tuple[int, int], re.Match[str]] = {}
        for pattern in (_CLOCK_COLON_RE, _CLOCK_POINT_RE):
            for match in pattern.finditer(segment):
                clocks[(match.start(), match.end())] = match
        clock_matches = sorted(clocks.values(), key=lambda item: item.start())
        for index, match in enumerate(dates):
            token = match.group(0)
            due_date = parse_due_date(token, run_date)
            if due_date is None:
                if _EXPLICIT_DATE_CANDIDATE_RE.fullmatch(token):
                    raise ValueError("invalid_due_date_candidate")
                continue
            next_date_start = dates[index + 1].start() if index + 1 < len(dates) else len(segment)
            clock_match = next(
                (
                    item for item in clock_matches
                    if match.end() <= item.start() < next_date_start
                    and item.start() - match.end() <= 24
                ),
                None,
            )
            due_time = time(18, 0)
            if clock_match is not None:
                due_time = _parse_clock_time(clock_match.group(0))
                if due_time is None:
                    raise ValueError("invalid_due_time_candidate")
            parsed_day = date.fromisoformat(due_date)
            due_iso = datetime.combine(parsed_day, due_time).strftime("%Y-%m-%dT%H:%M:%S+08:00")
            identity = (due_date, due_iso)
            if identity in seen:
                continue
            seen.add(identity)
            candidate={"key": f"date_{len(result)}", "due_date": due_date, "due_iso": due_iso}
            if include_context:
                candidate["source_context"]=segment.strip()
                span_start = max(0, match.start() - 120)
                span_end = min(len(segment), match.end() + 120)
                candidate["source_span"] = segment[span_start:span_end].strip()
                candidate["source_span_start"] = leading_whitespace + segment_start + span_start
                candidate["source_span_end"] = leading_whitespace + segment_start + span_end
                # Only assign a role when the date's local clause provides clear
                # evidence. Whole-segment keywords mislabel unrelated dates.
                left = segment[max(0, match.start() - 24):match.start()]
                right = segment[match.end():min(len(segment), match.end() + 36)]
                role_context = left + right
                effective = bool(re.search(r"(?:生效|终止|停止|暂停|启用|恢复|开始值守)", role_context))
                deadline = bool(re.search(r"(?:截止|截至|最晚|下班前|前.{0,18}(?:完成|反馈|回复|提交|上报|更新|落实|核对)|务必在|须在)",role_context))
                historical = bool(re.search(r"(?:联单时间|联单日期|处置日期|处置时间)", role_context))
                # Keep this a local date-plus-event construction; arbitrary
                # inspection words elsewhere in the clause do not change a
                # parser-unknown date into an event date.
                scheduled_event = bool(_SCHEDULED_EVENT_RE.match(right))
                conflicting_roles = (
                    (effective and deadline)
                    or (historical and (effective or deadline or scheduled_event))
                    or (scheduled_event and deadline)
                )
                candidate["role"] = (
                    "unknown" if conflicting_roles
                    else "historical" if historical
                    else "effective" if effective
                    else "deadline" if deadline
                    else "event" if scheduled_event
                    else "unknown"
                )
                candidate["deadline_context"] = candidate["role"] == "deadline"
            result.append(candidate)
    return result


def assemble_faithful_title(object_span: str | None, intent_span: str | None, *, max_length: int = 80) -> str | None:
    """Join cited source spans; refuse missing, oversized, or invented title parts."""
    if not isinstance(object_span, str) or not isinstance(intent_span, str):
        return None
    obj = object_span.strip(" ：:，,；;")
    intent = intent_span.strip(" ：:，,；;")
    if not obj or not intent or obj not in intent and intent not in obj:
        # Both components must be source-backed, but can be separate exact spans.
        pass
    title = f"{obj}：{intent}" if obj and intent and obj != intent else obj or intent
    if not title or len(title) > max_length:
        return None
    return title


def derive_title(text: str) -> tuple[str, bool]:
    """Return (title, needs_llm). Long notices are never silently truncated."""
    compact = normalize_text(text)
    compact = re.sub(r"^(通知|工作安排|请注意)\s*[:：]?\s*", "", compact)
    parts = re.split(r"[。！？\n]", compact, maxsplit=1)
    title = parts[0].strip(" ：:，,；;")
    if not title:
        return "", True
    if len(title) > 80 or ("附件" in compact and ("具体事项" in compact or "见群内" in compact)):
        return "", True
    return title, False


def detect_route(
    text: str,
    responsibility_rules: dict[str, str] | None = None,
) -> tuple[str | None, str | None, list[str]]:
    """Return module, exact responsibility name, and matched signals."""
    text = _as_text(text)
    responsibility_rules = responsibility_rules or {}
    for module, default_owner, signals in ROUTES:
        matched = [signal for signal in signals if signal in text]
        if matched:
            return module, responsibility_rules.get(module, default_owner), matched
    return None, None, []


def infer_semantic_context(text: str, source_group: str) -> dict[str, Any]:
    """Normalize workgroup scope before responsibility routing."""
    text = _as_text(text)
    source_group = _as_text(source_group)
    source_group_is_local = source_group in LOCAL_WORKGROUP_SOURCE_GROUPS
    all_workgroups = any(token in text for token in ("各工班", "所有工班", "各班组"))
    member_scope = any(token in text for token in ("铁炉西工班成员", "工班成员", "工班人员"))
    department_reference = any(token in text for token in DEPARTMENT_SCOPE_TERMS)
    text_mentions_workgroup = "铁炉西工班" in text
    explicit_local_target = text_mentions_workgroup or all_workgroups or member_scope
    if explicit_local_target:
        canonical = "铁炉西工班"
        target_scope = "managed_workgroup" if text_mentions_workgroup else "all_workgroups"
    elif department_reference:
        canonical = None
        target_scope = "department_only"
    elif source_group_is_local:
        canonical = "铁炉西工班"
        target_scope = "managed_workgroup"
    else:
        canonical = None
        target_scope = "unspecified"
    return {
        "canonical_workgroup": canonical,
        "target_scope": target_scope,
        "targeted_workgroups": ["铁炉西工班"] if canonical else [],
        "member_scope": member_scope,
        "department_module_reference": department_reference,
        "source_group_is_local": source_group_is_local,
    }


def _explicit_staff_names(text: str, staff: dict[str, dict[str, Any]]) -> list[str]:
    return [name for name in staff if name and name in text]


SELF_ASSIGNMENT_ROLE_TERMS = ("各总库工班长", "总库工班长")
ALL_SCOPE_TERMS = ("@所有人", "@全体人员", "@全体", "所有人", "全体人员")
WORKGROUP_COVERAGE_TERMS = ("铁炉西工班", "各工班", "所有工班", "各班组", "工班成员", "工班人员")


def _has_all_scope_mention(message: dict[str, Any], text: str) -> bool:
    compact = normalize_text(text)
    if any(term in compact for term in ALL_SCOPE_TERMS):
        return True
    return any(
        any(term in name for term in ("所有人", "全体人员", "全体"))
        for name in _at_names(message)
    )


def _covered_self_and_workgroup(
    message: dict[str, Any], semantic_context: dict[str, Any]
) -> bool:
    """Recognize a group-wide notice that explicitly covers this workgroup."""
    text = _as_text(message.get("text") or message.get("content"))
    source_group = _as_text(message.get("group") or message.get("source_group"))
    explicit_workgroup = any(term in text for term in WORKGROUP_COVERAGE_TERMS)
    local_source = source_group in LOCAL_WORKGROUP_SOURCE_GROUPS
    return explicit_workgroup or (_has_all_scope_mention(message, text) and local_source)


def _has_substantive_work_object(text: str) -> bool:
    compact = normalize_text(text)
    compact = re.sub(r"\[图片消息\]\([^)]*\)", "", compact)
    compact = re.sub(r"@[^\s]+(?:\([^)]*\))?", "", compact)
    compact = compact.replace("@所有人", "").replace("@全体人员", "")
    compact = compact.strip(" ：:，,；;。！？!?~")
    compact = re.sub(r"^(通知|工作安排|请注意)\s*[:：]?\s*", "", compact)
    return bool(compact)


def _at_names(message: dict[str, Any]) -> set[str]:
    """Collect structured @ names from normalized or raw DWS payloads."""
    values: list[Any] = []
    for key in ("at_users", "atUsers", "mentions", "mentionUsers", "at_list", "atList"):
        if message.get(key) is not None:
            values.append(message.get(key))
        raw = message.get("raw_payload")
        if isinstance(raw, dict) and raw.get(key) is not None:
            values.append(raw.get(key))
    names: set[str] = set()
    def visit(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for key in ("name", "nick", "nickName", "displayName", "userName", "staffName"):
                if value.get(key):
                    names.add(_as_text(value[key]))
            for key in ("atUsers", "at_users", "mentions", "users"):
                if value.get(key) is not None:
                    visit(value[key])
        elif value not in (None, ""):
            names.add(_as_text(value))
    for value in values:
        visit(value)
    return {item for item in names if item}


def _self_assignment(message: dict[str, Any], self_user: dict[str, Any]) -> tuple[bool, str]:
    """Return whether the message directly assigns the current user.

    A direct @mention is stronger than a generic module route.  In this
    workflow “各总库工班长” is the user's established role alias.
    """
    text = _as_text(message.get("text") or message.get("content"))
    self_name = _as_text(self_user.get("name"))
    self_id = _as_text(self_user.get("user_id") or self_user.get("userId"))
    structured = _at_names(message)
    raw = message.get("raw_payload")
    structured_ids: set[str] = set()
    if isinstance(raw, dict):
        for key in ("at_users", "atUsers", "mentions", "mentionUsers"):
            value = raw.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        for id_key in ("userId", "user_id", "staffId", "openId", "open_id"):
                            if item.get(id_key):
                                structured_ids.add(_as_text(item[id_key]))
    if self_name and (self_name in structured or (self_id and self_id in structured_ids)):
        return True, "structured_mention"
    if self_name and re.search(r"@\s*" + re.escape(self_name) + r"(?:\b|\(|$)", text):
        return True, "direct_mention"
    if any(term in text for term in SELF_ASSIGNMENT_ROLE_TERMS):
        return True, "role_alias"
    # A plain name reference together with a workgroup reference is also a
    # direct assignment in this workflow, even when the payload lost the
    # structured @ metadata.
    if self_name and self_name in text and any(term in text for term in ("工班", "总库")):
        return True, "self_and_workgroup_reference"
    return False, ""


VALID_LLM_SEMANTIC_INTENTS = frozenset(
    {
        "follow_up",
        "information_only",
        "acknowledgement",
        "completion_report",
        "opinion_request",
        "test",
        "uncertain",
    }
)


def _llm_semantic_intent(message: dict[str, Any]) -> str:
    """Read only the validated, private semantic hint from the LLM adapter."""
    value = _as_text(message.get("_llm_semantic_intent")).lower()
    return value if value in VALID_LLM_SEMANTIC_INTENTS else ""


def build_source_comment(message: dict[str, Any]) -> str:
    """Preserve the source group and original notice for a future todo comment."""
    group = _as_text(message.get("group") or message.get("source_group"))
    sender = _as_text(message.get("sender")) or "未标明"
    create_time = _as_text(message.get("create_time") or message.get("createTime")) or "未标明"
    message_id = _as_text(message.get("message_id") or message.get("messageId")) or "未标明"
    original = message.get("text")
    if original is None:
        original = message.get("content", "")
    return (
        f"来源群：{group}\n"
        f"发送人：{sender}\n"
        f"消息时间：{create_time}\n"
        f"messageId：{message_id}\n"
        f"原文：\n{original}"
    )


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def _source_key(message: dict[str, Any]) -> str:
    conversation_id = _as_text(message.get("conversation_id") or message.get("conversationId"))
    message_id = _as_text(message.get("message_id") or message.get("messageId"))
    return f"{conversation_id}:{message_id}:todo"


def plan_message(
    message: dict[str, Any],
    config: dict[str, Any],
    self_user: dict[str, str],
    *,
    run_date: date,
    mode: str = "test",
    live_ids: dict[str, str] | None = None,
    existing_actions: Iterable[dict[str, Any]] | None = None,
    allow_group_send: bool = False,
) -> dict[str, Any]:
    """Create a deterministic task plan; never performs an external write."""
    live_ids = live_ids or {}
    existing_actions = list(existing_actions or [])
    staff = config.get("staff") or {}
    responsibility_rules = config.get("responsibility_rules") or {}
    assign_to_self_only = config.get("assign_to_self_only") is True
    routing_loaded = bool(config.get("active_routing_loaded")) or assign_to_self_only
    text = _as_text(message.get("text") or message.get("content"))
    source_group = _as_text(message.get("group") or message.get("source_group"))
    self_assigned, assignment_basis = _self_assignment(message, self_user)
    semantic_intent = _llm_semantic_intent(message)
    semantic_context = infer_semantic_context(text, source_group)
    if message.get("_jev_v2_applicable"):
        semantic_context = {**semantic_context, "canonical_workgroup":"铁炉西工班", "target_scope":"managed_workgroup", "targeted_workgroups":["铁炉西工班"]}
    covered_self_and_workgroup = _covered_self_and_workgroup(message, semantic_context)
    if self_assigned:
        # An explicit @ to the current user (or the established role alias)
        # is a direct assignment, including when the surrounding text uses a
        # broad department or unspecified scope.
        semantic_context = {
            **semantic_context,
            "canonical_workgroup": "铁炉西工班",
            "target_scope": "explicit_self_assignment",
            "targeted_workgroups": ["铁炉西工班"],
            "explicit_self_assignment": True,
        }
    local_scope = semantic_context.get("target_scope") in {
        "managed_workgroup", "all_workgroups", "explicit_self_assignment"
    } or covered_self_and_workgroup
    try:
        confirmed_rule_id = classify_rule(
            text,
            source_group=source_group,
            local_scope=local_scope,
        )
        contract_version = "self-assignment/2026-11-03" if assign_to_self_only else load_contract()["contract_version"]
    except AssignmentContractError:
        confirmed_rule_id = None
        contract_version = "self-assignment/2026-11-03" if assign_to_self_only else None
    source_key = _source_key(message)
    result: dict[str, Any] = {
        "router_version": ROUTER_VERSION,
        "decision": "REVIEW",
        "source_key": source_key,
        "source_group": source_group,
        "message_id": _as_text(message.get("message_id") or message.get("messageId")),
        "conversation_id": _as_text(message.get("conversation_id") or message.get("conversationId")),
        "title": "",
        "detail": build_source_comment(message),
        "due_date": None,
        "due_iso": None,
        "priority": 20,
        "route_owner": None,
        "route_module": None,
        "matrix_route_owner": None,
        "assignment_target": "self" if self_assigned else ("self_and_workgroup" if covered_self_and_workgroup else None),
        "assignment_basis": assignment_basis or ("workgroup_all_mention" if covered_self_and_workgroup else None),
        "semantic_intent": semantic_intent or None,
        "matched_signals": [],
        "semantic_context": semantic_context,
        "workgroup_handling": "UNCONFIRMED",
        "routing_source": "active_config" if routing_loaded else "skill_baseline",
        "rule_contract_version": contract_version,
        "assignment_audit": None,
        "executor_ids": [],
        "executor_names": [],
        "routed_executor_names": [],
        "missing_fields": [],
        "warnings": [],
        "notify_group": bool(allow_group_send and mode == "production"),
        "comment": build_source_comment(message),
    }
    if contract_version is None:
        result.update(decision="REVIEW", reason="confirmed_assignment_contract_invalid")
        result["warnings"].append("confirmed_assignment_contract_invalid")
        return result
    # Forwarding an instruction or @-ing everyone does not assign it back to
    # its author. Keep authored messages visible for human routing, but never
    # create an external self-todo from them automatically.
    sender_id = _as_text(message.get("sender_id") or message.get("senderId"))
    sender_name = _as_text(message.get("sender") or message.get("sender_name"))
    self_id = _as_text(self_user.get("user_id") or self_user.get("userId"))
    self_name = _as_text(self_user.get("name"))
    sender_namespace_self_id = _as_text(self_user.get("message_sender_id"))
    verified_self_authored = bool(sender_id and sender_namespace_self_id and sender_id == sender_namespace_self_id)
    same_display_name = bool(sender_name and self_name and sender_name == self_name)
    if verified_self_authored:
        result["warnings"].append("self_authored_requires_review")
        # A verified own all-hands dispatch is authored for others, not a task
        # to recreate for its sender. Stop before semantic/LLM routing.
        if _has_all_scope_mention(message, text):
            return {**result, "decision": "IGNORE", "reason": "self_authored_notice"}
    if same_display_name and not verified_self_authored:
        result["warnings"].append("self_authored_requires_review")
        if sender_id:
            result["warnings"].append("self_authorship_unverified")

    source_conversation_id = result["conversation_id"]
    source_message_id = result["message_id"]
    for existing in existing_actions:
        same_source = existing.get("source_key") == source_key or (
            _as_text(existing.get("action_key")) in {"", "todo"}
            and source_conversation_id
            and source_message_id
            and _as_text(existing.get("conversation_id")) == source_conversation_id
            and _as_text(existing.get("message_id")) == source_message_id
        )
        if same_source:
            return {**result, "decision": "IGNORE", "reason": "source_already_processed"}

    if not source_group:
        result["missing_fields"].append("source_group")
    if not result["message_id"] or not result["conversation_id"]:
        result["missing_fields"].append("source_identity")
    if not text:
        result["missing_fields"].append("source_text")

    title = _as_text(message.get("title"))
    if not title and text:
        title, title_needs_llm = derive_title(text)
        if title_needs_llm:
            result["warnings"].append("title_needs_llm")
    if self_assigned and title:
        title = re.sub(r"\[图片消息\]\([^)]*\)", "", title)
        title = re.sub(r"@[^\s]+(?:\([^)]*\))?", "", title).strip(" ：:，,；;")
    if message.get("_jev_v2_applicable") and text:
        source_text = re.sub(r"\[图片消息\]\([^)]*\)", "", text)
        source_text = re.sub(r"@[^\s]+(?:\([^)]*\))?", "", source_text).strip()
        source_text = re.sub(r"^(通知|工作安排|请注意)\s*[:：]?\s*", "", source_text)
        request_parts = re.split(r"(?<=[。！？])", source_text)
        if len(request_parts) > 1 and re.search(r"(?:已完成|已经完成|完成了|已办结|已结束)", request_parts[0]):
            source_text = "".join(request_parts[1:]).strip()
        # For short source text, keep the complete source instead of selecting
        # a sentence that may misrepresent other actions or quoted context.
        request = source_text.strip(" 。！？：:，,；;") if len(source_text) <= 80 else None
        title = assemble_faithful_title(request, request)
        result["warnings"] = [w for w in result["warnings"] if w != "title_needs_llm"]
        if title is None:
            title = ""
            result["warnings"].append("title_requires_context")
    elif not title and self_assigned and text:
        fallback = re.sub(r"\[图片消息\]\([^)]*\)", "", text)
        fallback = re.sub(r"@[^\s]+(?:\([^)]*\))?", "", fallback)
        fallback = normalize_text(fallback)
        fallback = re.sub(r"^(通知|工作安排|请注意)\s*[:：]?\s*", "", fallback)
        fallback = re.split(r"[。！？\n]", fallback, maxsplit=1)[0].strip(" ：:，,；;")
        if fallback and len(fallback) <= 80:
            title = fallback
            result["warnings"] = [w for w in result["warnings"] if w != "title_needs_llm"]
    if message.get("_jev_v2_title_verified") and _as_text(message.get("title")):
        title = _as_text(message.get("title"))
        result["warnings"] = [w for w in result["warnings"] if w not in {"title_needs_llm", "title_requires_context"}]
    result["title"] = title
    if not title:
        result["missing_fields"].append("title")

    if "信息测试" in text or text.strip().startswith("测试"):
        result.update(decision="IGNORE", reason="test_message")
        return result
    if text and _is_completion_report(text):
        result.update(decision="IGNORE", reason="completion_report")
        return result
    awareness_notice = confirmed_rule_id == "D"
    if text and not _has_substantive_work_object(text):
        result.update(decision="IGNORE", reason="no_action_signal")
        return result
    if text and not any(term in text for term in ACTION_TERMS) and (self_assigned or covered_self_and_workgroup) and not message.get("_jev_v2_applicable") and not awareness_notice:
        result["warnings"].append("explicit_action_missing")
        return result
    semantic_resolution_needed = bool(
        text
        and not any(term in text for term in ACTION_TERMS)
        and not self_assigned
        and not covered_self_and_workgroup
        and semantic_intent != "follow_up"
        and not awareness_notice
    )
    if semantic_resolution_needed and not semantic_intent:
        # A candidate without an explicit action is not a proven non-task.
        # Preserve it for review/semantic interpretation; scope gates below
        # still deterministically reject department-only and unconfirmed items.
        result["warnings"].append("semantic_intent_needs_llm")
    if text and any(text == ack or text.startswith(ack) for ack in ACK_TERMS) and not any(
        term in text for term in ACTION_TERMS[2:]
    ):
        result.update(decision="IGNORE", reason="acknowledgement_only")
        return result
    if semantic_context["target_scope"] == "department_only" and not self_assigned and not covered_self_and_workgroup:
        result["workgroup_handling"] = "NON_LOCAL"
        result.update(decision="IGNORE", reason="department_scope_non_local")
        return result
    if semantic_context["target_scope"] == "unspecified" and not self_assigned:
        result["workgroup_handling"] = "UNCONFIRMED"
        result["warnings"].append("workgroup_scope_unconfirmed")
        result["missing_fields"].append("workgroup_scope")
        return result

    if semantic_resolution_needed:
        result.update(decision="REVIEW", reason="semantic_resolution_required")
        return result
    if semantic_intent in {"information_only", "opinion_request", "uncertain"} and not any(
        term in text for term in ACTION_TERMS
    ):
        result.update(decision="REVIEW", reason="semantic_review_required")
        return result

    result["workgroup_handling"] = (
        "DIRECT_SELF"
        if self_assigned
        else "COVERED_SELF_WORKGROUP"
        if covered_self_and_workgroup
        else "LOCAL"
    )

    module, owner, matched = detect_route(text, responsibility_rules)
    matrix_owner = owner
    if self_assigned:
        owner = _as_text(self_user.get("name")) or owner
        if not module:
            module = "direct_assignment"
        if "explicit_self_assignment" not in matched:
            matched = [*matched, "explicit_self_assignment"]
    elif covered_self_and_workgroup:
        owner = _as_text(self_user.get("name")) or owner
        if not module:
            module = "covered_workgroup"
        if "workgroup_all_mention" not in matched:
            matched = [*matched, "workgroup_all_mention"]
    if assign_to_self_only:
        owner = _as_text(self_user.get("name")) or None
        matrix_owner = None
        module = module or "workgroup_follow_up"
    elif message.get("_jev_v2_applicable") and module == "hazardous_waste":
        # In the new policy, only a runtime-supplied responsibility matrix is authoritative.
        owner = _as_text(responsibility_rules.get("hazardous_waste")) or None
        matrix_owner = owner
    elif message.get("_jev_v2_applicable"):
        # New policy's default is李林骁; generic safety/training/drill/asset
        # matrix routing must not silently replace the policy default.
        owner = _as_text(self_user.get("name")) or None
    if message.get("_contextual_followup_verified") is CONTEXTUAL_FOLLOWUP_VERIFIED_TOKEN:
        owner = _as_text(self_user.get("name")) or None
        module = module or "workgroup_follow_up"
    if not owner and message.get("_jev_v2_applicable") and module != "hazardous_waste":
        owner = _as_text(self_user.get("name")) or None
        module = module or "workgroup_follow_up"

    confirmed_assignment: dict[str, Any] | None = None
    if confirmed_rule_id and not assign_to_self_only:
        try:
            confirmed_assignment = resolve_confirmed_assignment(
                rule_id=confirmed_rule_id,
                text=text,
                config=config,
                run_date=run_date,
            )
        except AssignmentContractError:
            result.update(decision="REVIEW", reason="confirmed_assignment_contract_invalid")
            result["warnings"].append("confirmed_assignment_contract_invalid")
            return result
        result["assignment_audit"] = confirmed_assignment["audit"]
        result["rule_contract_version"] = confirmed_assignment["audit"]["rule_version"]
        if confirmed_rule_id in {"A", "C"}:
            audit = result["assignment_audit"]
            audit["matrix_candidate_owner"] = matrix_owner
            selected_owner = confirmed_assignment.get("primary_owner")
            if matrix_owner and selected_owner and matrix_owner != selected_owner:
                audit["database_conflict"] = {
                    "configured_owner": matrix_owner,
                    "selected_owner": selected_owner,
                }
                audit["conflict_resolution"] = (
                    "explicit_current_assignment_has_priority_over_confirmed_static_rule_and_legacy_matrix"
                    if audit.get("assignment_basis") == "explicit_named_assignment_in_source"
                    else "confirmed_static_rule_has_priority_over_legacy_matrix"
                )
                audit["legacy_configuration_recheck_required"] = True
        module = confirmed_assignment["module"]
        owner = confirmed_assignment.get("primary_owner")
        result["routed_executor_names"] = list(confirmed_assignment.get("executor_names") or [])
        result["routing_source"] = "confirmed_assignment_contract"
        if confirmed_assignment.get("manual_assignment_required"):
            result.update(
                decision="REVIEW",
                reason="manual_assignment_required",
                route_module=module,
                route_owner=None,
                matrix_route_owner=matrix_owner,
            )
            result["missing_fields"].append("manual_assignment")
            result["matched_signals"] = matched
            return result
        if confirmed_assignment.get("reason"):
            result.update(
                decision="REVIEW",
                reason=confirmed_assignment["reason"],
                route_module=module,
                route_owner=None,
                matrix_route_owner=matrix_owner,
            )
            result["warnings"].append(confirmed_assignment["reason"])
            if not routing_loaded:
                result["warnings"].append("active_routing_not_loaded")
            result["missing_fields"].append("current_area_owner")
            result["matched_signals"] = matched
            result["handover_evidence_reason"] = confirmed_assignment.get("missing_evidence_reason")
            return result
        if awareness_notice:
            owner = None
            result["route_owner"] = None
        elif owner and not result["routed_executor_names"]:
            result["routed_executor_names"] = [owner]
        result["matched_signals"] = [*matched, f"confirmed_rule_{confirmed_rule_id}"]
    result["route_module"] = module
    result["route_owner"] = owner
    result["matrix_route_owner"] = matrix_owner
    if not confirmed_rule_id or assign_to_self_only:
        result["matched_signals"] = matched
    if not routing_loaded and not confirmed_assignment:
        result["warnings"].append("active_routing_not_loaded")
    if not owner and not awareness_notice:
        result["warnings"].append("responsibility_route_missing")
        result["missing_fields"].append("route_owner")
    if not confirmed_assignment and owner:
        result["routed_executor_names"] = [owner]
        if owner != self_name:
            result["routed_executor_names"].append(self_name)

    explicit_due_date = _as_text(message.get("due_date"))
    if explicit_due_date:
        due_date = explicit_due_date
        due_iso = _as_text(message.get("due_iso")) or f"{due_date}T18:00:00+08:00"
    elif message.get("_due_date_selection_resolved"):
        due_date, due_iso = None, None
    else:
        due_date, due_iso = parse_due_datetime(text, run_date)
    result["due_date"] = due_date or None
    if due_date:
        result["due_iso"] = due_iso
    elif not self_assigned and not covered_self_and_workgroup and not message.get("_jev_v2_applicable") and not awareness_notice:
        result["missing_fields"].append("due_date")

    explicit_names = _explicit_staff_names(text, staff)
    if owner and any(name != owner for name in explicit_names) and not self_assigned and not assign_to_self_only:
        result["warnings"].append("owner_conflict")

    normalized = normalize_text(title)
    if normalized:
        # A model-generated title can be identical for unrelated notices. Bind
        # the business claim to the source text so that a title collision does
        # not attach a new message to an unrelated historical action.
        source_day = _as_text(message.get("create_time"))[:10]
        business_key = _hash_key("|".join((normalized, owner or "", due_date or "", module or "", source_day, normalize_text(text))))
        result["business_key"] = business_key
        for existing in existing_actions:
            if existing.get("business_key") == business_key:
                # A local hash can locate a candidate, but it cannot prove that a
                # current DWS task is active or represents the same scope/round.
                # The production writer performs full DWS search and detail
                # reconciliation before creating; source-key idempotency remains
                # an independent gate.
                result["local_business_candidate"] = True
                break

    self_name = _as_text(self_user.get("name"))
    self_id = _as_text(self_user.get("user_id"))
    if self_name != SELF_NAME or not self_id:
        result["warnings"].append("self_identity_not_verified")
    if not self_id:
        result["missing_fields"].append("self_user_id")

    if result["missing_fields"] or result["warnings"]:
        return result

    if mode == "test":
        # Before final launch, the only actual executor is the owner himself.
        result["executor_ids"] = [self_id]
        result["executor_names"] = [self_name]
    elif mode == "production":
        routed_names = list(dict.fromkeys(result.get("routed_executor_names") or []))
        if awareness_notice and not routed_names:
            routed_names = list((result.get("assignment_audit") or {}).get("awareness_names") or [])
        if owner and self_name not in routed_names and not confirmed_assignment:
            routed_names.append(self_name)
        missing_ids = [name for name in routed_names if not _as_text(live_ids.get(name))]
        ids = [_as_text(live_ids.get(name)) for name in routed_names]
        if missing_ids or not routed_names or len(set(ids)) != len(ids):
            result["warnings"].append(
                "external_executor_identity_unverified" if not missing_ids else "external_owner_id_missing"
            )
            return result
        result["routed_executor_names"] = routed_names
        result["executor_ids"] = ids
        result["executor_names"] = routed_names
    else:
        result["warnings"].append("unknown_mode")
        return result

    result["decision"] = "CREATE"
    return result
