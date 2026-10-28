# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import datetime

import pendulum
import pytest

from airflow.timetables._cron import CronMixin, _localize_all_folds

SAMPLE_TZ = "UTC"


def test_valid_cron_expression():
    cm = CronMixin("* * 1 * *", SAMPLE_TZ)  # every day at midnight
    assert isinstance(cm.description, str)
    assert "Every minute" in cm.description or "month" in cm.description


def test_invalid_cron_expression():
    cm = CronMixin("invalid cron", SAMPLE_TZ)
    assert cm.description == ""


def test_dom_and_dow_conflict():
    cm = CronMixin("* * 1 * 1", SAMPLE_TZ)  # 1st of month or Monday
    desc = cm.description

    assert "(or)" in desc
    assert "Every minute, on day 1 of the month" in desc
    assert "Every minute, only on Monday" in desc


@pytest.mark.parametrize(
    ("timezone", "friday_last_tick", "monday_first_tick"),
    [
        pytest.param(
            "Europe/Berlin",
            pendulum.datetime(2026, 10, 23, 21, 0, tz="UTC"),  # Fri 23:00 CEST
            pendulum.datetime(2026, 10, 25, 23, 0, tz="UTC"),  # Mon 00:00 CET
            id="berlin-fall-back",
        ),
        pytest.param(
            "America/Chicago",
            pendulum.datetime(2026, 10, 31, 4, 0, tz="UTC"),  # Fri 2026-10-30 23:00 CDT
            pendulum.datetime(2026, 11, 2, 6, 0, tz="UTC"),  # Mon 00:00 CST
            id="chicago-fall-back",
        ),
        pytest.param(
            "Australia/Sydney",
            pendulum.datetime(2026, 4, 3, 12, 0, tz="UTC"),  # Fri 23:00 AEDT
            pendulum.datetime(2026, 4, 5, 14, 0, tz="UTC"),  # Mon 00:00 AEST
            id="sydney-fall-back",
        ),
        pytest.param(
            "Europe/Berlin",
            pendulum.datetime(2027, 3, 26, 22, 0, tz="UTC"),  # Fri 23:00 CET
            pendulum.datetime(2027, 3, 28, 22, 0, tz="UTC"),  # Mon 00:00 CEST
            id="berlin-spring-forward",
        ),
    ],
)
def test_weekday_hourly_across_weekend_dst(timezone, friday_last_tick, monday_first_tick):
    mixin = CronMixin("0 * * * 1-5", timezone)

    assert mixin._get_next(friday_last_tick) == monday_first_tick
    assert mixin._get_prev(monday_first_tick) == friday_last_tick


@pytest.mark.parametrize(
    ("timezone", "cron", "current", "expected_next"),
    [
        pytest.param(
            "America/Havana",
            "0 * * * 0",
            pendulum.datetime(2026, 10, 26, 3, 0, tz="UTC"),
            pendulum.datetime(2026, 11, 1, 4, 0, tz="UTC"),  # Sun 00:00 -04:00, first of the folded hour
            id="havana-target-in-fold-hour",
        ),
    ],
)
def test_get_next_keeps_earlier_occurrence_in_fold_hour(timezone, cron, current, expected_next):
    assert CronMixin(cron, timezone)._get_next(current) == expected_next


@pytest.mark.parametrize(
    ("timezone", "cron", "current", "expected_prev"),
    [
        pytest.param(
            "America/Godthab",
            "0 * * * 6",
            pendulum.datetime(2026, 4, 4, 1, 0, tz="UTC"),
            pendulum.datetime(2026, 3, 29, 0, 0, tz="UTC"),  # Sat 22:00 -02:00; Sat 23:00 is skipped
            id="godthab-target-in-gap",
        ),
    ],
)
def test_get_prev_skips_wall_clock_time_in_spring_forward_gap(timezone, cron, current, expected_prev):
    assert CronMixin(cron, timezone)._get_prev(current) == expected_prev


@pytest.mark.parametrize(
    ("timezone", "naive", "expected"),
    [
        pytest.param(
            "UTC",
            datetime.datetime(2026, 6, 1, 12, 0),
            [pendulum.datetime(2026, 6, 1, 12, 0, tz="UTC")],
            id="ordinary-time",
        ),
        pytest.param(
            "America/Havana",
            datetime.datetime(2026, 11, 1, 0, 0),
            [pendulum.datetime(2026, 11, 1, 4, 0, tz="UTC"), pendulum.datetime(2026, 11, 1, 5, 0, tz="UTC")],
            id="havana-fold",
        ),
        pytest.param(
            "Europe/London",
            datetime.datetime(2026, 10, 25, 1, 30),
            [
                pendulum.datetime(2026, 10, 25, 0, 30, tz="UTC"),
                pendulum.datetime(2026, 10, 25, 1, 30, tz="UTC"),
            ],
            id="london-fold",
        ),
        pytest.param(
            "America/Godthab",
            datetime.datetime(2026, 3, 28, 23, 0),
            [],
            id="godthab-spring-forward-gap",
        ),
    ],
)
def test_localize_all_folds(timezone, naive, expected):
    assert _localize_all_folds(naive, pendulum.timezone(timezone)) == expected
