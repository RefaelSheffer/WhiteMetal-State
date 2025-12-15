import pytest

from scripts.update_data import decide_action


def test_decide_action_respects_custom_thresholds():
    high_buy, conf_buy = decide_action(25, buy_threshold=20, sell_threshold=-40)
    assert high_buy == "BUY / ADD"
    assert conf_buy == "HIGH"

    reduce, conf_reduce = decide_action(-35, buy_threshold=40, sell_threshold=-30)
    assert reduce == "REDUCE / HEDGE"
    assert conf_reduce == "HIGH"

    bullish, conf_bullish = decide_action(5, bullish_hold_threshold=5, bearish_hold_threshold=-5)
    assert bullish == "HOLD (Bullish bias)"
    assert conf_bullish == "MED"
