"""CSV 解析、数值/时刻校验与浓度计算。

业务规则集中在本模块，不依赖 Web 层，便于单独测试。
"""

from __future__ import annotations

import csv
import io
import json
import re
import sys
from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from .errors import InputError

MAX_DATA_ROWS = 500
MAX_MESSAGES = 40
MAX_NUMBER_TEXT = 4000
MAX_ABS_ADJUSTED_EXPONENT = 10000
MAX_RESULT_TEXT = 20000
# 极端但合法的输入组合仍可能生成超长十进制文本；需要时可安全提高该上限。
_MIN_INT_DIGITS = sys.get_int_max_str_digits()
if _MIN_INT_DIGITS and _MIN_INT_DIGITS < 200000:
    sys.set_int_max_str_digits(200000)

_NUMBER_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_UTC_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?)"
    r"(?:Z|[+-]\d{2}:?\d{2})?$",
    re.IGNORECASE,
)

SAMPLE_HEADER_ALIASES = {
    "sample_id": {"样本号", "样本编号", "样品号", "sampleid", "sample", "id"},
    "timestamp": {
        "utc时刻",
        "utc时间",
        "时刻utc",
        "时间utc",
        "采样时刻",
        "samplingtime",
        "utctime",
        "timestamp",
        "time",
    },
    "absorbance": {"吸光度", "吸光值", "absorbance", "abs"},
}

CAL_HEADER_ALIASES = {
    "start": {
        "起始utc时刻",
        "起始utc时间",
        "开始utc时刻",
        "开始utc时间",
        "起始时刻",
        "开始时刻",
        "起始时间",
        "startutc",
        "starttime",
        "start",
        "from",
    },
    "end": {
        "结束utc时刻",
        "结束utc时间",
        "截止utc时刻",
        "结束时刻",
        "截止时刻",
        "结束时间",
        "endutc",
        "endtime",
        "end",
        "to",
    },
    "slope": {"斜率", "slope", "k"},
    "intercept": {"截距", "intercept", "b"},
}


@dataclass(frozen=True)
class Sample:
    row_number: int
    sample_id: str
    timestamp_text: str
    timestamp: datetime
    absorbance: Decimal


@dataclass(frozen=True)
class Calibration:
    row_number: int
    start: datetime
    end: datetime
    slope: Decimal
    intercept: Decimal


@dataclass(frozen=True)
class ResultRow:
    sample_id: str
    timestamp_text: str
    absorbance: str
    slope: str
    intercept: str
    concentration: str


def _normalize_header(value: str) -> str:
    return re.sub(r"[\s_\-]+", "", value.strip().lower()).replace("（", "").replace("）", "")


def _lookup_header(header: list[str], aliases: dict[str, set[str]], kind: str) -> dict[str, int]:
    normalized = [_normalize_header(cell) for cell in header]
    found: dict[str, int] = {}
    problems: list[str] = []
    for field, names in aliases.items():
        positions = [i for i, name in enumerate(normalized) if name in names]
        if not positions:
            problems.append(f"{kind}缺少必需列“{min(names)}”")
        elif len(positions) > 1:
            problems.append(f"{kind}存在重复的“{header[positions[0]]}”列")
        else:
            found[field] = positions[0]
    if problems:
        raise InputError(problems)
    return found


def _decode_csv(data: bytes) -> str:
    if not data:
        raise InputError("上传的 CSV 为空")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise InputError("CSV 必须使用 UTF-8 编码") from None
    if "\x00" in text:
        raise InputError("CSV 含非法空字符")
    return text


def _read_rows(data: bytes, kind: str) -> tuple[list[str], list[tuple[int, list[str]]]]:
    text = _decode_csv(data)
    try:
        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        rows = list(reader)
    except csv.Error as exc:
        raise InputError(f"{kind} CSV 格式错误：{exc}") from None
    if not rows:
        raise InputError(f"{kind} CSV 缺少表头")

    header = [cell.strip() for cell in rows[0]]
    if not any(header):
        raise InputError(f"{kind} CSV 缺少表头")

    data_rows: list[tuple[int, list[str]]] = []
    for index, row in enumerate(rows[1:], start=2):
        if not row:  # 完全空行忽略
            continue
        if len(row) != len(header):
            raise InputError(
                f"{kind} 第 {index} 行有 {len(row)} 个字段，表头有 {len(header)} 个字段"
            )
        data_rows.append((index, row))
    if not data_rows:
        raise InputError(f"{kind} CSV 没有数据行")
    if len(data_rows) > MAX_DATA_ROWS:
        raise InputError(f"{kind} CSV 数据行数为 {len(data_rows)}，超过上限 {MAX_DATA_ROWS}")
    return header, data_rows


def parse_positive_revision(value: object) -> int:
    if value is None:
        raise InputError("修订号不能为空")
    text = str(value).strip()
    if not re.fullmatch(r"\d+", text):
        raise InputError("修订号必须是正整数")
    try:
        revision = int(text)
    except ValueError:
        raise InputError("修订号必须是正整数") from None
    if revision <= 0:
        raise InputError("修订号必须是正整数")
    if revision > 2**63 - 1:
        raise InputError("修订号超出可存储范围")
    return revision


def parse_decimal(value: str, label: str):
    text = value.strip()
    if not text:
        raise InputError(f"{label}不能为空")
    if len(text) > MAX_NUMBER_TEXT:
        raise InputError(f"{label}数值过长")
    if not _NUMBER_RE.fullmatch(text):
        raise InputError(f"{label}“{text}”不是有限小数")
    try:
        number = Decimal(text)
    except InvalidOperation:
        raise InputError(f"{label}“{text}”不是有限小数") from None
    if not number.is_finite():
        raise InputError(f"{label}“{text}”不是有限小数")
    adjusted = number.adjusted() if number != 0 else 0
    if abs(adjusted) > MAX_ABS_ADJUSTED_EXPONENT:
        raise InputError(f"{label}“{text}”数值超出允许范围")
    return number


def decimal_plain(number: Decimal) -> str:
    """输出不带科学计数法、无多余尾零的有限小数。"""
    if number == 0:
        return "0"
    sign = "-" if number < 0 else ""
    text = format(abs(number), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return sign + (text or "0")


def fraction_plain(value: Fraction) -> str:
    """把分母仅含 2/5 的分数精确转换成十进制文本。"""
    if value == 0:
        return "0"
    sign = "-" if value < 0 else ""
    numerator = abs(value.numerator)
    denominator = value.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise ValueError("结果不是有限小数")
    scale = max(twos, fives)
    if scale > MAX_RESULT_TEXT:
        raise InputError("计算结果的小数位数超出允许范围")
    numerator *= 2 ** (scale - twos) * 5 ** (scale - fives)
    digits = str(numerator)
    if scale == 0:
        if len(digits) > MAX_RESULT_TEXT:
            raise InputError("计算结果超出允许范围")
        return sign + digits
    digits = digits.rjust(scale + 1, "0")
    integer = digits[:-scale] or "0"
    fraction = digits[-scale:].rstrip("0")
    result = sign + integer + ("." + fraction if fraction else "")
    if len(result) > MAX_RESULT_TEXT + MAX_ABS_ADJUSTED_EXPONENT:
        raise InputError("计算结果超出允许范围")
    return result


def parse_utc(value: str, label: str) -> datetime:
    text = value.strip()
    if not text:
        raise InputError(f"{label}不能为空")
    match = _UTC_RE.fullmatch(text)
    if not match:
        raise InputError(
            f"{label}“{text}”不是有效 UTC 时刻（示例：2024-01-01T00:00:00Z）"
        )
    normalized = text
    if normalized[-1:] in ("Z", "z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise InputError(f"{label}“{text}”不是有效 UTC 时刻") from None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_utc(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    text = normalized.isoformat(timespec="microseconds")
    if text.endswith("+00:00"):
        text = text[:-6] + "Z"
    if "." in text:
        head, tail = text.split(".", 1)
        tail = tail[:-1].rstrip("0")
        text = head + ("." + tail if tail else "") + "Z"
    return text


def _fail(messages: list[str]):
    if len(messages) > MAX_MESSAGES:
        extra = len(messages) - MAX_MESSAGES
        messages = messages[:MAX_MESSAGES] + [f"……另有 {extra} 项错误"]
    raise InputError(messages)


def parse_samples(data: bytes) -> list[Sample]:
    header, rows = _read_rows(data, "原始样本")
    columns = _lookup_header(header, SAMPLE_HEADER_ALIASES, "原始样本")

    samples: list[Sample] = []
    errors: list[str] = []
    seen_ids: dict[str, int] = {}
    for line_number, row in rows:
        sample_id = row[columns["sample_id"]].strip()
        timestamp_text = row[columns["timestamp"]].strip()
        absorbance_text = row[columns["absorbance"]].strip()
        if not sample_id:
            errors.append(f"第 {line_number} 行样本号为空")
            continue
        if sample_id in seen_ids:
            errors.append(
                f"第 {line_number} 行样本号“{sample_id}”重复（首次出现在第 {seen_ids[sample_id]} 行）"
            )
            continue
        seen_ids[sample_id] = line_number
        try:
            timestamp = parse_utc(timestamp_text, "UTC 时刻")
        except InputError as exc:
            errors.append(f"第 {line_number} 行：{exc.messages[0]}")
            continue
        try:
            absorbance = parse_decimal(absorbance_text, "吸光度")
        except InputError as exc:
            errors.append(f"第 {line_number} 行：{exc.messages[0]}")
            continue
        samples.append(
            Sample(
                row_number=line_number,
                sample_id=sample_id,
                timestamp_text=timestamp_text,
                timestamp=timestamp,
                absorbance=absorbance,
            )
        )
    if errors:
        _fail(errors)
    return samples


def parse_calibrations(data: bytes) -> list[Calibration]:
    header, rows = _read_rows(data, "校准")
    columns = _lookup_header(header, CAL_HEADER_ALIASES, "校准")

    calibrations: list[Calibration] = []
    errors: list[str] = []
    for line_number, row in rows:
        try:
            start = parse_utc(row[columns["start"]], "起始 UTC 时刻")
            end = parse_utc(row[columns["end"]], "结束 UTC 时刻")
            slope = parse_decimal(row[columns["slope"]], "斜率")
            intercept = parse_decimal(row[columns["intercept"]], "截距")
        except InputError as exc:
            errors.append(f"第 {line_number} 行：{exc.messages[0]}")
            continue
        if start >= end:
            errors.append(f"第 {line_number} 行校准区间起始时刻必须早于结束时刻")
            continue
        calibrations.append(
            Calibration(
                row_number=line_number,
                start=start,
                end=end,
                slope=slope,
                intercept=intercept,
            )
        )
    if errors:
        _fail(errors)

    # 半开区间 [start, end)，起始含、结束不含；相邻区间端点相等不算重叠。
    ordered = sorted(calibrations, key=lambda item: (item.start, item.end))
    latest_end = None
    latest_row = None
    for item in ordered:
        if latest_end is not None and item.start < latest_end:
            errors.append(
                f"校准区间重叠：第 {latest_row} 行与第 {item.row_number} 行在 "
                f"{iso_utc(item.start)} 附近重叠"
            )
        if latest_end is None or item.end > latest_end:
            latest_end = item.end
            latest_row = item.row_number
    if errors:
        _fail(errors)
    return ordered


def calibration_payload(calibrations: Iterable[Calibration]) -> list[dict[str, str]]:
    """规范化校准内容，用于判断“同号是否异内容”。"""
    payload = [
        {
            "start": iso_utc(item.start),
            "end": iso_utc(item.end),
            "slope": decimal_plain(item.slope),
            "intercept": decimal_plain(item.intercept),
        }
        for item in calibrations
    ]
    payload.sort(key=lambda item: (item["start"], item["end"], item["slope"], item["intercept"]))
    return payload


def payload_json(payload: list[dict[str, str]]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_results(samples: Iterable[Sample], calibrations: list[Calibration]) -> list[ResultRow]:
    """按 [start, end) 匹配校准段，精确计算浓度；任何问题整批拒绝。"""
    starts = [item.start for item in calibrations]
    results: list[ResultRow] = []
    errors: list[str] = []
    for sample in samples:
        index = bisect_right(starts, sample.timestamp) - 1
        calibration = calibrations[index] if index >= 0 else None
        if calibration is None or not (calibration.start <= sample.timestamp < calibration.end):
            errors.append(
                f"样本“{sample.sample_id}”的时刻 {sample.timestamp_text} 未命中任何校准区间"
            )
            continue
        slope = Fraction(calibration.slope)
        intercept = Fraction(calibration.intercept)
        concentration = slope * Fraction(sample.absorbance) + intercept
        if concentration < 0:
            errors.append(
                f"样本“{sample.sample_id}”的浓度为负（{fraction_plain(concentration)}）"
            )
            continue
        results.append(
            ResultRow(
                sample_id=sample.sample_id,
                timestamp_text=sample.timestamp_text,
                absorbance=decimal_plain(sample.absorbance),
                slope=decimal_plain(calibration.slope),
                intercept=decimal_plain(calibration.intercept),
                concentration=fraction_plain(concentration),
            )
        )
    if errors:
        _fail(errors)
    return results


RESULT_HEADERS = ["样本号", "UTC时刻", "吸光度", "斜率", "截距", "浓度"]


def results_to_csv(results: Iterable[ResultRow]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(RESULT_HEADERS)
    for row in results:
        writer.writerow(
            [
                row.sample_id,
                row.timestamp_text,
                row.absorbance,
                row.slope,
                row.intercept,
                row.concentration,
            ]
        )
    return buffer.getvalue().encode("utf-8")
