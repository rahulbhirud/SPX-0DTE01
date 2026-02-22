"""Tests for _build_occ_symbol in options_chain_scheduler.py."""

from datetime import date

from options_chain_scheduler import _build_occ_symbol


class TestSchedulerBuildOccSymbol:
    def test_put_symbol(self):
        sym = _build_occ_symbol("$SPXW.X", date(2026, 2, 23), "Put", 6775.0)
        assert sym == "SPXW 260223P6775"

    def test_call_symbol(self):
        sym = _build_occ_symbol("$SPXW.X", date(2026, 2, 23), "Call", 6980.0)
        assert sym == "SPXW 260223C6980"

    def test_fractional_strike(self):
        sym = _build_occ_symbol("$SPXW.X", date(2026, 2, 23), "Put", 6775.5)
        assert sym == "SPXW 260223P6775.5"

    def test_plain_root(self):
        sym = _build_occ_symbol("SPXW", date(2026, 3, 1), "Call", 7000.0)
        assert sym == "SPXW 260301C7000"

    def test_accepts_date_object(self):
        d = date(2026, 12, 18)
        sym = _build_occ_symbol("$SPX.X", d, "Put", 5500.0)
        assert sym == "SPX 261218P5500"
