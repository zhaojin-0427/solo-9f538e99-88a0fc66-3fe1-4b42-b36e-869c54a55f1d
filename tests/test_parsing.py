"""解析与数值计算单元测试。"""

from decimal import Decimal
from fractions import Fraction

import pytest

from lab.errors import InputError
from lab.parsing import (
    compute_results,
    decimal_plain,
    fraction_plain,
    iso_utc,
    parse_calibrations,
    parse_samples,
    parse_utc,
    results_to_csv,
)


def test_plain_decimal_formatting():
    assert decimal_plain(Decimal("0.5000")) == "0.5"
    assert decimal_plain(Decimal("1E+3")) == "1000"
    assert decimal_plain(Decimal("-0.000")) == "0"
    assert decimal_plain(Decimal("0.00001")) == "0.00001"


def test_fraction_plain_exact():
    assert fraction_plain(Fraction(1, 8)) == "0.125"
    assert fraction_plain(Fraction(-3, 2)) == "-1.5"
    with pytest.raises(ValueError):
        fraction_plain(Fraction(1, 3))


def test_exact_decimal_arithmetic_avoids_binary_float_errors():
    samples = parse_samples(b"sample_id,timestamp,absorbance\nS,2024-01-01T00:00:00Z,0.1\n")
    calibrations = parse_calibrations(
        b"start,end,slope,intercept\n2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,1,0.2\n"
    )
    rows = compute_results(samples, calibrations)
    assert rows[0].concentration == "0.3"


def test_utc_offsets_are_converted():
    assert iso_utc(parse_utc("2024-01-01T08:00:00+08:00", "t")) == "2024-01-01T00:00:00Z"
    assert iso_utc(parse_utc("2024-01-01 00:00:00", "t")) == "2024-01-01T00:00:00Z"
    assert iso_utc(parse_utc("2024-01-01T00:00:00.500Z", "t")) == "2024-01-01T00:00:00.5Z"


def test_csv_bom_crlf_and_quoted_fields():
    samples = parse_samples(
        '样本号,UTC时刻,吸光度\r\n"S 1",2024-01-01T00:00:00Z,"1.5"\r\n'.encode("utf-8-sig")
    )
    assert samples[0].sample_id == "S 1"
    assert decimal_plain(samples[0].absorbance) == "1.5"


def test_empty_and_short_rows_are_rejected():
    with pytest.raises(InputError):
        parse_samples(b"sample_id,timestamp,absorbance\nS1,2024-01-01T00:00:00Z\n")
    with pytest.raises(InputError):
        parse_samples(b"sample_id,timestamp,absorbance\n")
    with pytest.raises(InputError):
        parse_samples(b"")


def test_overlap_detection_across_multiple_segments():
    data = (
        b"start,end,slope,intercept\n"
        b"2024-01-01T00:00:00Z,2024-01-01T06:00:00Z,1,0\n"
        b"2024-01-01T06:00:00Z,2024-01-01T12:00:00Z,1,0\n"
        b"2024-01-01T05:00:00Z,2024-01-01T07:00:00Z,1,0\n"
    )
    with pytest.raises(InputError) as excinfo:
        parse_calibrations(data)
    assert "重叠" in str(excinfo.value)


def test_zero_width_calibration_rejected():
    with pytest.raises(InputError):
        parse_calibrations(
            b"start,end,slope,intercept\n2024-01-01T00:00:00Z,2024-01-01T00:00:00Z,1,0\n"
        )


def test_result_csv_round_trip_shape():
    samples = parse_samples(b"sample_id,timestamp,absorbance\nS,2024-01-01T00:00:00Z,2\n")
    calibrations = parse_calibrations(
        b"start,end,slope,intercept\n2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,0.5,1\n"
    )
    content = results_to_csv(compute_results(samples, calibrations)).decode()
    assert content == "样本号,UTC时刻,吸光度,斜率,截距,浓度\nS,2024-01-01T00:00:00Z,2,0.5,1,2\n"


def test_extreme_magnitudes_rejected_not_crashing():
    samples = parse_samples(b"sample_id,timestamp,absorbance\nS,2024-01-01T00:00:00Z,1\n")
    for slope in ("1e999999999", "1e-999999999", "9" * 4001):
        # 明显越界的数量级在解析阶段即被拒绝。
        with pytest.raises(InputError):
            parse_calibrations(
                f"start,end,slope,intercept\n2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,{slope},0\n".encode()
            )
    # 位数很长但数量级仍可表示的输入不会崩溃或退化为科学计数法。
    for slope in ("1e9999", "9" * 3000):
        calibrations = parse_calibrations(
            f"start,end,slope,intercept\n2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,{slope},0\n".encode()
        )
        concentration = compute_results(samples, calibrations)[0].concentration
        assert "E" not in concentration
        assert set(concentration) <= set("0123456789.-")


def test_huge_but_representable_results_are_finite():
    samples = parse_samples(b"sample_id,timestamp,absorbance\nS,2024-01-01T00:00:00Z,1\n")
    calibrations = parse_calibrations(
        b"start,end,slope,intercept\n2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,1e300,1e300\n"
    )
    rows = compute_results(samples, calibrations)
    assert "E" not in rows[0].concentration
    assert rows[0].concentration.startswith("2")
