from typing import List, Dict, Any
from datetime import datetime

def aggregate_signals(signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregates multiple signals from different strategies using pre-configured weights.
    Additionally, if an RSI signal is available, adjust other signals’ weights.
    """
    if not signals:
        return {"signal_type": "HOLD", "action": "HOLD", "signal": "HOLD", "confidence": 0.0}

    # Check if an RSI signal is present.
    rsi_signal = next((s for s in signals if s.get("source") == "RSI"), None)
    weight_map = {"legacy": 1.0, "MACD": 1.0, "Bollinger": 1.0, "ML": 0.6, "RSI": 1.0}
    buy_conf = 0.0
    sell_conf = 0.0
    for s in signals:
        src = s.get("source", "legacy")
        weight = weight_map.get(src, 1.0)
        # If an RSI signal is present, adjust weight for non-RSI signals.
        if rsi_signal and src != "RSI":
            if rsi_signal.get("action") == s.get("action"):
                weight *= 1.2
            else:
                weight *= 0.8
        conf = s.get("confidence", 0) * weight
        if s.get("action") == "BUY":
            buy_conf += conf
        elif s.get("action") == "SELL":
            sell_conf += conf
    if buy_conf == 0 and sell_conf == 0:
        return {"signal_type": "HOLD", "action": "HOLD", "signal": "HOLD", "confidence": 0.0}
    if buy_conf > sell_conf:
        action = "BUY"
        agg_conf = buy_conf / (buy_conf + sell_conf)
    elif sell_conf > buy_conf:
        action = "SELL"
        agg_conf = sell_conf / (buy_conf + sell_conf)
    else:
        action = "HOLD"
        agg_conf = 0.0
    return {
        "symbol": signals[0].get("symbol", "UNKNOWN"),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "signal_type": action,
        "action": action,
        "signal": action,
        "confidence": round(agg_conf, 2)
    }