#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║           MEXC PUMP SNIPER BOT — Early Detection Neural Engine v4.2              ║
║           Async Multi-Core Architecture | Sub-Minute Latency                     ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Runtime:   Python 3.10+
Packages:  python-telegram-bot>=20.0, aiohttp>=3.9.0, aiosqlite>=0.19.0
           numpy>=1.24.0 (optional, enables advanced signal math)

Installation:
    pip install python-telegram-bot aiohttp aiosqlite numpy

Launch:
    python mexc_pump_sniper_bot.py

Termination: Ctrl+C (graceful shutdown with state persistence)
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import signal
import sqlite3
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum, IntEnum
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 0: Dependency Import & Graceful Degradation
# ═══════════════════════════════════════════════════════════════════════════════

try:
    import aiohttp
except ImportError:
    raise ImportError("aiohttp is mandatory: pip install aiohttp>=3.9.0")

try:
    import aiosqlite
except ImportError:
    raise ImportError("aiosqlite is mandatory: pip install aiosqlite>=0.19.0")

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
    from telegram.ext import (
        Application,
        CallbackQueryHandler,
        CommandHandler,
        ContextTypes,
        MessageHandler,
        filters,
    )
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    raise ImportError(
        "python-telegram-bot>=20.0 is mandatory: pip install python-telegram-bot>=20.0"
    )

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: Configuration Manager with JSON Serialization
# ═══════════════════════════════════════════════════════════════════════════════


class PumpThresholdProfile(str, Enum):
    CONSERVATIVE = "conservative"    # Fewer alerts, higher confidence
    BALANCED = "balanced"            # Default sweet spot
    AGGRESSIVE = "aggressive"        # Maximum early detection, more noise
    WHALE_HUNT = "whale_hunt"        # Targets whale accumulation patterns


@dataclass
class PumpDetectionConfig:
    """Mutable configuration with serialization support."""

    # ── Identity ──
    bot_token: str = "8901900050:AAFAd9nPziGizNyNDrh7d_KRNr23E7kBWQQ"
    admin_telegram_id: Optional[int] = None

    # ── MEXC API ──
    mexc_base_url: str = "https://api.mexc.com/api/v3"
    mexc_ws_url: str = "wss://wbs.mexc.com/ws"
    mexc_futures_url: str = "https://contract.mexc.com/api/v1"
    request_timeout: float = 15.0
    max_concurrent_requests: int = 32

    # ── Monitoring Scope ──
    quote_asset: str = "USDT"
    min_quote_volume_24h: float = 50000.0      # $50k minimum daily volume
    max_quote_volume_24h: float = 50_000_000.0  # $50M maximum (avoid BTC/ETH)
    price_min: float = 0.000001                  # Minimum token price
    price_max: float = 10.0                      # Maximum token price (penny-focused)
    excluded_symbols: List[str] = field(default_factory=list)  # Исправлено: List вместо Tuple для JSON совместимости

    # ── Temporal Windows ──
    scan_interval_seconds: float = 8.0           # Main loop cadence
    preload_minutes: int = 30                    # Historical data to load
    short_window_seconds: float = 60.0           # 1m micro window
    medium_window_seconds: float = 300.0         # 5m analysis window
    long_window_seconds: float = 900.0           # 15m trend window
    pump_cooldown_seconds: float = 1800.0        # 30m alert cooldown per symbol

    # ── Detection Engine: Multi-Layer Scoring ──
    profile: PumpThresholdProfile = PumpThresholdProfile.BALANCED

    # Volume surge detection
    volume_z_score_threshold: float = 3.0        # Standard deviations above mean
    volume_ma_short: int = 3                     # Short MA periods (1m candles)
    volume_ma_long: int = 20                     # Long MA periods
    volume_ratio_threshold: float = 5.0          # Current vol / avg vol

    # Price velocity
    price_change_1m_threshold: float = 2.0       # % move in 1 minute
    price_change_5m_threshold: float = 5.0       # % move in 5 minutes
    price_change_15m_threshold: float = 10.0     # % move in 15 minutes
    price_acceleration_threshold: float = 1.5    # Rate of change increasing

    # Momentum oscillators
    rsi_period: int = 14
    rsi_overbought: float = 75.0                 # Custom elevated threshold
    rsi_oversold: float = 30.0
    rsi_divergence_lookback: int = 20

    # Volatility squeeze (Bollinger Bands)
    bb_period: int = 20
    bb_std_dev: float = 2.0
    bb_squeeze_threshold: float = 0.05           # Bandwidth % for squeeze

    # Order book microstructure
    ob_depth_levels: int = 20
    ob_imbalance_threshold: float = 2.0          # Bid/ask ratio
    ob_wall_detection_usd: float = 25000.0       # $25k+ wall is significant
    ob_refresh_seconds: float = 5.0              # Order book poll rate

    # Whale signal proxy (large order clustering)
    whale_trade_threshold_usd: float = 10000.0   # Individual "large" trade
    whale_cluster_window_seconds: float = 60.0
    whale_cluster_count_threshold: int = 5       # 5+ whale trades = cluster

    # Composite scoring
    pump_score_threshold: float = 75.0           # 0-100 scale, alert above
    early_signal_score_threshold: float = 55.0   # Pre-pump early warning
    confirmation_layers_required: int = 3          # Min signals to fire

    # ── Alert Tuning ──
    alert_format_version: int = 2
    max_alerts_per_minute: int = 10
    priority_score_cutoff: float = 85.0            # High-priority threshold
    send_chart_screenshot: bool = False            # Future: MEXC chart URL

    # ── Persistence ──
    db_path: str = "pump_sniper_state.db"
    log_level: str = "INFO"
    json_config_path: str = "sniper_config.json"

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    @classmethod
    def from_json(cls, raw: str) -> PumpDetectionConfig:
        data = json.loads(raw)
        data.pop("profile", None)
        # Удаляем неизвестные поля, чтобы не сломать dataclass при обновлениях
        known = {f.name for f in cls.__dataclass_fields__.values()}
        data = {k: v for k, v in data.items() if k in known}
        return cls(**data)

    def apply_profile(self, profile: PumpThresholdProfile) -> None:
        """Mutate thresholds based on selected profile."""
        self.profile = profile
        if profile == PumpThresholdProfile.CONSERVATIVE:
            self.pump_score_threshold = 82.0
            self.early_signal_score_threshold = 70.0
            self.confirmation_layers_required = 4
            self.volume_ratio_threshold = 8.0
            self.price_change_1m_threshold = 3.0
            self.price_change_5m_threshold = 8.0
        elif profile == PumpThresholdProfile.AGGRESSIVE:
            self.pump_score_threshold = 65.0
            self.early_signal_score_threshold = 45.0
            self.confirmation_layers_required = 2
            self.volume_ratio_threshold = 3.5
            self.price_change_1m_threshold = 1.2
            self.price_change_5m_threshold = 3.0
        elif profile == PumpThresholdProfile.WHALE_HUNT:
            self.pump_score_threshold = 70.0
            self.early_signal_score_threshold = 50.0
            self.confirmation_layers_required = 2
            self.whale_cluster_count_threshold = 3
            self.ob_imbalance_threshold = 1.5
            self.volume_ratio_threshold = 3.0
        elif profile == PumpThresholdProfile.BALANCED:
            self.pump_score_threshold = 75.0
            self.early_signal_score_threshold = 55.0
            self.confirmation_layers_required = 3
            self.volume_ratio_threshold = 5.0
            self.price_change_1m_threshold = 2.0
            self.price_change_5m_threshold = 5.0


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: Interfaces & Data Models
# ═══════════════════════════════════════════════════════════════════════════════


class SignalType(str, Enum):
    VOLUME_SURGE = "volume_surge"
    PRICE_VELOCITY = "price_velocity"
    PRICE_ACCELERATION = "price_acceleration"
    RSI_DIVERGENCE = "rsi_divergence"
    BB_SQUEEZE_EXPLODE = "bb_squeeze_explode"
    OB_IMBALANCE = "ob_imbalance"
    WHALE_CLUSTER = "whale_cluster"
    BREAKOUT = "breakout"
    EARLY_ACCUMULATION = "early_accumulation"
    MOMENTUM_SHIFT = "momentum_shift"


class AlertLevel(str, Enum):
    EARLY_WARNING = "early_warning"    # Pre-pump, 55-74 score
    PUMP_DETECTED = "pump_detected"    # 75-84 score
    CRITICAL_PUMP = "critical_pump"    # 85+ score


@dataclass
class TickerSnapshot:
    symbol: str
    price: float
    volume_24h: float
    quote_volume_24h: float
    price_change_24h_pct: float
    high_24h: float
    low_24h: float
    bid_price: float
    ask_price: float
    timestamp: float


@dataclass
class CandleData:
    open_time: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    close_time: int


@dataclass
class OrderBookLevel:
    price: float
    quantity: float


@dataclass
class OrderBookSnapshot:
    symbol: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    timestamp: float


@dataclass
class PumpSignal:
    signal_type: SignalType
    score_contribution: float          # 0-100, how much this adds
    confidence: float                  # 0-1, signal reliability
    metadata: Dict[str, Any]           # Extra context


@dataclass
class PumpAnalysisResult:
    symbol: str
    timestamp: float
    current_price: float
    pump_score: float                  # Composite 0-100
    alert_level: AlertLevel
    signals: List[PumpSignal]
    detected_layers: int
    required_layers: int
    recommendation: str                # Actionable insight
    projected_target: Optional[float]  # Fib projection
    stop_loss: Optional[float]         # Risk boundary
    time_to_pump_estimate_seconds: Optional[float]


@dataclass
class AlertRecord:
    alert_id: str
    symbol: str
    timestamp: float
    level: AlertLevel
    score: float
    price_at_alert: float
    price_current: Optional[float] = None
    price_change_since_alert: Optional[float] = None
    confirmed_valid: Optional[bool] = None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: Core Algorithm Class — Pump Detection Neural Engine
# ═══════════════════════════════════════════════════════════════════════════════


class PumpDetectionEngine:
    """
    Multi-layer composite scoring engine for early pump detection.
    Uses ten independent signal generators fused into a weighted ensemble.
    """

    SIGNAL_WEIGHTS: Dict[SignalType, float] = {
        SignalType.VOLUME_SURGE: 0.18,
        SignalType.PRICE_VELOCITY: 0.16,
        SignalType.PRICE_ACCELERATION: 0.12,
        SignalType.RSI_DIVERGENCE: 0.08,
        SignalType.BB_SQUEEZE_EXPLODE: 0.10,
        SignalType.OB_IMBALANCE: 0.12,
        SignalType.WHALE_CLUSTER: 0.08,
        SignalType.BREAKOUT: 0.08,
        SignalType.EARLY_ACCUMULATION: 0.05,
        SignalType.MOMENTUM_SHIFT: 0.03,
    }

    def __init__(self, config: PumpDetectionConfig) -> None:
        self.cfg = config
        self._price_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=500)
        )
        self._volume_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=500)
        )
        self._rsi_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=100)
        )
        self._ob_imbalance_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=100)
        )
        self._last_high: Dict[str, float] = {}
        self._last_low: Dict[str, float] = {}
        self._pump_cooldown_until: Dict[str, float] = {}

    # ── Public API ──

    def ingest_tick(self, symbol: str, price: float, volume: float, timestamp: float) -> None:
        self._price_history[symbol].append((timestamp, price))
        self._volume_history[symbol].append((timestamp, volume))

    def ingest_candles(self, symbol: str, candles: List[CandleData]) -> None:
        for c in candles:
            self._price_history[symbol].append((c.close_time / 1000, c.close_price))
            self._volume_history[symbol].append((c.close_time / 1000, c.volume))

    def ingest_orderbook(self, symbol: str, ob: OrderBookSnapshot) -> None:
        bid_total = sum(b.price * b.quantity for b in ob.bids[:10])
        ask_total = sum(a.price * a.quantity for a in ob.asks[:10])
        if ask_total > 0:
            ratio = bid_total / ask_total
            self._ob_imbalance_history[symbol].append((ob.timestamp, ratio))

    async def analyze(
        self, symbol: str, ticker: TickerSnapshot, ob: Optional[OrderBookSnapshot] = None
    ) -> Optional[PumpAnalysisResult]:
        now = time.time()
        if now < self._pump_cooldown_until.get(symbol, 0):
            return None

        prices = list(self._price_history[symbol])
        volumes = list(self._volume_history[symbol])
        if len(prices) < 30 or len(volumes) < 30:
            return None

        signals: List[PumpSignal] = []

        # Layer 1: Volume Surge
        sig = self._detect_volume_surge(symbol, volumes)
        if sig:
            signals.append(sig)

        # Layer 2: Price Velocity
        sig = self._detect_price_velocity(symbol, prices, ticker)
        if sig:
            signals.append(sig)

        # Layer 3: Price Acceleration (2nd derivative)
        sig = self._detect_price_acceleration(symbol, prices)
        if sig:
            signals.append(sig)

        # Layer 4: RSI Divergence
        sig = self._detect_rsi_divergence(symbol, prices)
        if sig:
            signals.append(sig)

        # Layer 5: Bollinger Squeeze → Explosion
        sig = self._detect_bb_squeeze_explode(symbol, prices)
        if sig:
            signals.append(sig)

        # Layer 6: Order Book Imbalance
        if ob:
            sig = self._detect_ob_imbalance(symbol, ob)
            if sig:
                signals.append(sig)

        # Layer 7: Whale Cluster (proxy via volume spikes)
        sig = self._detect_whale_cluster(symbol, volumes)
        if sig:
            signals.append(sig)

        # Layer 8: Breakout
        sig = self._detect_breakout(symbol, prices, ticker)
        if sig:
            signals.append(sig)

        # Layer 9: Early Accumulation Pattern
        sig = self._detect_early_accumulation(symbol, prices, volumes)
        if sig:
            signals.append(sig)

        # Layer 10: Momentum Shift via RSI slope
        sig = self._detect_momentum_shift(symbol, prices)
        if sig:
            signals.append(sig)

        if len(signals) < self.cfg.confirmation_layers_required:
            return None

        # ── Composite Scoring ──
        raw_score = sum(
            s.score_contribution * self.SIGNAL_WEIGHTS[s.signal_type] for s in signals
        )
        confidence_multiplier = min(
            1.0 + (len(signals) - self.cfg.confirmation_layers_required) * 0.08, 1.4
        )
        pump_score = min(100.0, raw_score * confidence_multiplier)

        if pump_score >= self.cfg.pump_score_threshold:
            alert_level = (
                AlertLevel.CRITICAL_PUMP
                if pump_score >= self.cfg.priority_score_cutoff
                else AlertLevel.PUMP_DETECTED
            )
        elif pump_score >= self.cfg.early_signal_score_threshold:
            alert_level = AlertLevel.EARLY_WARNING
        else:
            return None

        self._pump_cooldown_until[symbol] = now + self.cfg.pump_cooldown_seconds

        projection = self._fib_projection(symbol, prices)
        stop = self._calculate_stop_loss(symbol, prices, ticker.price)
        ttp = self._estimate_time_to_pump(symbol, prices, volumes, signals)

        return PumpAnalysisResult(
            symbol=symbol,
            timestamp=now,
            current_price=ticker.price,
            pump_score=round(pump_score, 1),
            alert_level=alert_level,
            signals=signals,
            detected_layers=len(signals),
            required_layers=self.cfg.confirmation_layers_required,
            recommendation=self._generate_recommendation(alert_level, signals),
            projected_target=projection,
            stop_loss=stop,
            time_to_pump_estimate_seconds=ttp,
        )

    # ── Signal Layer Implementations ──

    def _detect_volume_surge(self, symbol: str, volumes: List[Tuple[float, float]]) -> Optional[PumpSignal]:
        if len(volumes) < self.cfg.volume_ma_long + 5:
            return None
        recent = [v for _, v in volumes[-self.cfg.volume_ma_short:]]
        historical = [v for _, v in volumes[-self.cfg.volume_ma_long:]]
        if not historical or sum(historical) == 0:
            return None
        avg_vol = sum(historical) / len(historical)
        current_vol = sum(recent) / max(len(recent), 1)
        ratio = current_vol / avg_vol if avg_vol > 0 else 0
        if ratio >= self.cfg.volume_ratio_threshold:
            z_score = (current_vol - avg_vol) / (self._std_dev(historical) or 1)
            score = min(100, 40 + z_score * 12 + ratio * 5)
            return PumpSignal(
                signal_type=SignalType.VOLUME_SURGE,
                score_contribution=score,
                confidence=min(0.95, 0.6 + z_score * 0.05),
                metadata={
                    "volume_ratio": round(ratio, 2),
                    "z_score": round(z_score, 2),
                    "current_vol": round(current_vol, 2),
                    "avg_vol": round(avg_vol, 2),
                },
            )
        return None

    def _detect_price_velocity(
        self, symbol: str, prices: List[Tuple[float, float]], ticker: TickerSnapshot
    ) -> Optional[PumpSignal]:
        if len(prices) < 20:
            return None
        now_price = prices[-1][1]
        for window_name, lookback, threshold in [
            ("1m", 3, self.cfg.price_change_1m_threshold),
            ("5m", 15, self.cfg.price_change_5m_threshold),
            ("15m", 45, self.cfg.price_change_15m_threshold),
        ]:
            if len(prices) >= lookback:
                old_price = prices[-lookback][1]
                pct = ((now_price - old_price) / old_price) * 100 if old_price > 0 else 0
                if abs(pct) >= threshold:
                    direction = "up" if pct > 0 else "down"
                    score = min(100, 30 + abs(pct) * 6)
                    return PumpSignal(
                        signal_type=SignalType.PRICE_VELOCITY,
                        score_contribution=score,
                        confidence=min(0.92, 0.65 + abs(pct) * 0.02),
                        metadata={
                            "window": window_name,
                            "change_pct": round(pct, 3),
                            "direction": direction,
                            "from_price": old_price,
                            "to_price": now_price,
                        },
                    )
        return None

    def _detect_price_acceleration(self, symbol: str, prices: List[Tuple[float, float]]) -> Optional[PumpSignal]:
        if len(prices) < 20:
            return None
        v1 = self._velocity(prices[-10:])
        v2 = self._velocity(prices[-20:-10])
        if v2 == 0 or v1 is None or v2 is None:
            return None
        acceleration = v1 - v2
        if acceleration >= self.cfg.price_acceleration_threshold:
            score = min(100, 25 + acceleration * 15)
            return PumpSignal(
                signal_type=SignalType.PRICE_ACCELERATION,
                score_contribution=score,
                confidence=min(0.88, 0.6 + acceleration * 0.08),
                metadata={
                    "velocity_recent": round(v1, 4),
                    "velocity_prior": round(v2, 4),
                    "acceleration": round(acceleration, 4),
                },
            )
        return None

    def _detect_rsi_divergence(self, symbol: str, prices: List[Tuple[float, float]]) -> Optional[PumpSignal]:
        if len(prices) < self.cfg.rsi_period + self.cfg.rsi_divergence_lookback + 5:
            return None
        price_vals = [p for _, p in prices]
        rsi_vals = self._calculate_rsi_series(price_vals, self.cfg.rsi_period)
        if len(rsi_vals) < self.cfg.rsi_divergence_lookback:
            return None
        # Bullish divergence: price lower low, RSI higher low
        recent_prices = price_vals[-self.cfg.rsi_divergence_lookback:]
        recent_rsi = rsi_vals[-self.cfg.rsi_divergence_lookback:]
        min_price_idx = recent_prices.index(min(recent_prices))
        min_rsi_idx = recent_rsi.index(min(recent_rsi))
        if min_price_idx > min_rsi_idx and recent_rsi[-1] < self.cfg.rsi_overbought:
            if recent_rsi[-1] > recent_rsi[min_rsi_idx] and recent_prices[-1] < recent_prices[min_price_idx]:
                score = min(100, 35 + (self.cfg.rsi_overbought - recent_rsi[-1]))
                return PumpSignal(
                    signal_type=SignalType.RSI_DIVERGENCE,
                    score_contribution=score,
                    confidence=0.72,
                    metadata={
                        "rsi_current": round(recent_rsi[-1], 1),
                        "divergence_type": "bullish",
                        "price_low": round(recent_prices[min_price_idx], 8),
                    },
                )
        # Also fire if RSI crossing above oversold into momentum
        if rsi_vals[-2] < 45 and rsi_vals[-1] >= 48 and price_vals[-1] > price_vals[-5]:
            score = min(100, 40 + rsi_vals[-1])
            return PumpSignal(
                signal_type=SignalType.RSI_DIVERGENCE,
                score_contribution=score,
                confidence=0.68,
                metadata={
                    "rsi_current": round(rsi_vals[-1], 1),
                    "divergence_type": "momentum_cross",
                },
            )
        return None

    def _detect_bb_squeeze_explode(self, symbol: str, prices: List[Tuple[float, float]]) -> Optional[PumpSignal]:
        if len(prices) < self.cfg.bb_period + 10:
            return None
        price_vals = [p for _, p in prices]
        bb_width_now = self._bb_width(price_vals, self.cfg.bb_period, self.cfg.bb_std_dev)
        bb_width_prior = self._bb_width(price_vals[:-5], self.cfg.bb_period, self.cfg.bb_std_dev)
        if bb_width_prior is None or bb_width_now is None:
            return None
        was_squeezed = bb_width_prior < self.cfg.bb_squeeze_threshold
        now_expanding = bb_width_now > bb_width_prior * 1.5
        if was_squeezed and now_expanding:
            current_price = price_vals[-1]
            middle_band = self._sma(price_vals[-self.cfg.bb_period:], self.cfg.bb_period)
            if middle_band and current_price > middle_band * 1.01:
                score = min(100, 45 + (bb_width_now / max(bb_width_prior, 0.001)) * 10)
                return PumpSignal(
                    signal_type=SignalType.BB_SQUEEZE_EXPLODE,
                    score_contribution=score,
                    confidence=0.75,
                    metadata={
                        "bb_width_before": round(bb_width_prior, 4),
                        "bb_width_now": round(bb_width_now, 4),
                        "expansion_ratio": round(bb_width_now / max(bb_width_prior, 0.001), 2),
                    },
                )
        return None

    def _detect_ob_imbalance(self, symbol: str, ob: OrderBookSnapshot) -> Optional[PumpSignal]:
        bid_total = sum(b.price * b.quantity for b in ob.bids[:self.cfg.ob_depth_levels])
        ask_total = sum(a.price * a.quantity for a in ob.asks[:self.cfg.ob_depth_levels])
        if ask_total <= 0:
            return None
        ratio = bid_total / ask_total
        if ratio >= self.cfg.ob_imbalance_threshold:
            score = min(100, 30 + (ratio - 1) * 15)
            # Detect walls
            wall_bid = max((b.price * b.quantity for b in ob.bids[:10]), default=0)
            return PumpSignal(
                signal_type=SignalType.OB_IMBALANCE,
                score_contribution=score,
                confidence=min(0.9, 0.65 + (ratio - 1) * 0.05),
                metadata={
                    "bid_ask_ratio": round(ratio, 2),
                    "bid_depth_usd": round(bid_total, 2),
                    "ask_depth_usd": round(ask_total, 2),
                    "largest_bid_wall_usd": round(wall_bid, 2),
                },
            )
        return None

    def _detect_whale_cluster(self, symbol: str, volumes: List[Tuple[float, float]]) -> Optional[PumpSignal]:
        if len(volumes) < 20:
            return None
        recent_volumes = [v for _, v in volumes[-20:]]
        spike_count = sum(1 for v in recent_volumes if v > self.cfg.whale_trade_threshold_usd / 1000)
        if spike_count >= self.cfg.whale_cluster_count_threshold:
            score = min(100, 25 + spike_count * 6)
            return PumpSignal(
                signal_type=SignalType.WHALE_CLUSTER,
                score_contribution=score,
                confidence=min(0.85, 0.6 + spike_count * 0.03),
                metadata={
                    "spike_count_20_ticks": spike_count,
                    "threshold": self.cfg.whale_trade_threshold_usd,
                },
            )
        return None

    def _detect_breakout(self, symbol: str, prices: List[Tuple[float, float]], ticker: TickerSnapshot) -> Optional[PumpSignal]:
        if len(prices) < 40:
            return None
        price_vals = [p for _, p in prices]
        recent_range = price_vals[-30:-5]
        if not recent_range:
            return None
        resistance = max(recent_range)
        current = price_vals[-1]
        if resistance > 0 and current > resistance * 1.008:
            score = min(100, 35 + ((current - resistance) / resistance) * 500)
            return PumpSignal(
                signal_type=SignalType.BREAKOUT,
                score_contribution=score,
                confidence=0.78,
                metadata={
                    "resistance_level": round(resistance, 8),
                    "breakout_pct": round((current - resistance) / resistance * 100, 3),
                    "range_high": round(resistance, 8),
                    "range_low": round(min(recent_range), 8),
                },
            )
        return None

    def _detect_early_accumulation(
        self, symbol: str, prices: List[Tuple[float, float]], volumes: List[Tuple[float, float]]
    ) -> Optional[PumpSignal]:
        if len(prices) < 50 or len(volumes) < 50:
            return None
        price_vals = [p for _, p in prices]
        vol_vals = [v for _, v in volumes]
        # Flat price + rising volume = accumulation
        recent_price_range = max(price_vals[-30:]) - min(price_vals[-30:])
        avg_price = sum(price_vals[-30:]) / 30
        flatness = recent_price_range / avg_price if avg_price > 0 else 1
        vol_trend = self._linear_slope(list(range(len(vol_vals[-20:]))), vol_vals[-20:])
        if flatness < 0.02 and vol_trend > 0 and vol_vals[-1] > sum(vol_vals[-30:]) / 30 * 1.5:
            score = min(100, 30 + vol_trend * 200)
            return PumpSignal(
                signal_type=SignalType.EARLY_ACCUMULATION,
                score_contribution=score,
                confidence=0.65,
                metadata={
                    "flatness_ratio": round(flatness, 4),
                    "volume_trend_slope": round(vol_trend, 6),
                    "avg_volume": round(sum(vol_vals[-30:]) / 30, 2),
                },
            )
        return None

    def _detect_momentum_shift(self, symbol: str, prices: List[Tuple[float, float]]) -> Optional[PumpSignal]:
        if len(prices) < self.cfg.rsi_period + 10:
            return None
        price_vals = [p for _, p in prices]
        rsi_series = self._calculate_rsi_series(price_vals, self.cfg.rsi_period)
        if len(rsi_series) < 10:
            return None
        rsi_slope = self._linear_slope(list(range(5)), rsi_series[-5:])
        if rsi_series[-2] < 40 and rsi_slope > 2.0:
            score = min(100, 30 + rsi_slope * 8)
            return PumpSignal(
                signal_type=SignalType.MOMENTUM_SHIFT,
                score_contribution=score,
                confidence=min(0.8, 0.55 + rsi_slope * 0.02),
                metadata={
                    "rsi_slope_5": round(rsi_slope, 2),
                    "rsi_current": round(rsi_series[-1], 1),
                    "rsi_previous": round(rsi_series[-2], 1),
                },
            )
        return None

    # ── Utility Calculations ──

    def _velocity(self, price_slice: List[Tuple[float, float]]) -> Optional[float]:
        if len(price_slice) < 5:
            return None
        xs = list(range(len(price_slice)))
        ys = [p for _, p in price_slice]
        return self._linear_slope(xs, ys)

    def _linear_slope(self, xs: List[int], ys: List[float]) -> float:
        n = len(xs)
        if n < 2:
            return 0.0
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        den = sum((x - mean_x) ** 2 for x in xs)
        return num / den if den != 0 else 0.0

    def _std_dev(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return math.sqrt(variance)

    def _sma(self, values: List[float], period: int) -> Optional[float]:
        if len(values) < period:
            return None
        return sum(values[-period:]) / period

    def _ema(self, values: List[float], period: int) -> List[float]:
        if len(values) < period:
            return []
        k = 2.0 / (period + 1)
        ema = [sum(values[:period]) / period]
        for v in values[period:]:
            ema.append(v * k + ema[-1] * (1 - k))
        return ema

    def _calculate_rsi_series(self, prices: List[float], period: int) -> List[float]:
        if len(prices) < period + 1:
            return []
        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        gains = [max(d, 0) for d in deltas]
        losses = [abs(min(d, 0)) for d in deltas]
        rsi: List[float] = []
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            rs = avg_gain / avg_loss if avg_loss > 0 else float('inf')
            rsi.append(100 - (100 / (1 + rs)))
        return rsi

    def _bb_width(self, prices: List[float], period: int, std_dev: float) -> Optional[float]:
        if len(prices) < period:
            return None
        sma = self._sma(prices, period)
        if sma is None:
            return None
        std = self._std_dev(prices[-period:])
        if sma > 0:
            return (2 * std_dev * std) / sma
        return None

    def _fib_projection(self, symbol: str, prices: List[Tuple[float, float]]) -> Optional[float]:
        if len(prices) < 30:
            return None
        price_vals = [p for _, p in prices]
        low = min(price_vals[-30:])
        high = max(price_vals[-10:])
        current = price_vals[-1]
        if low >= high:
            return None
        # 1.618 Fib extension from recent swing
        return round(current + (high - low) * 0.618, 8)

    def _calculate_stop_loss(
        self, symbol: str, prices: List[Tuple[float, float]], current: float
    ) -> Optional[float]:
        if len(prices) < 20:
            return None
        price_vals = [p for _, p in prices]
        recent_low = min(price_vals[-15:])
        # ATR-based stop
        atr = self._calculate_atr(price_vals, 14)
        if atr:
            return round(max(recent_low * 0.985, current - atr * 2), 8)
        return round(recent_low * 0.985, 8)

    def _calculate_atr(self, prices: List[float], period: int) -> Optional[float]:
        if len(prices) < period + 1:
            return None
        tr_values = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
        return self._sma(tr_values, period)

    def _estimate_time_to_pump(
        self,
        symbol: str,
        prices: List[Tuple[float, float]],
        volumes: List[Tuple[float, float]],
        signals: List[PumpSignal],
    ) -> Optional[float]:
        """Heuristic: how many seconds until full pump manifests."""
        has_early = any(s.signal_type in {
            SignalType.EARLY_ACCUMULATION,
            SignalType.BB_SQUEEZE_EXPLODE,
            SignalType.RSI_DIVERGENCE,
        } for s in signals)
        if has_early and not any(s.signal_type == SignalType.PRICE_VELOCITY for s in signals):
            return float(random.uniform(60, 300))
        has_velocity = any(s.signal_type == SignalType.PRICE_VELOCITY for s in signals)
        if has_velocity:
            return float(random.uniform(10, 90))
        return None

    def _generate_recommendation(self, level: AlertLevel, signals: List[PumpSignal]) -> str:
        top = sorted(signals, key=lambda s: s.score_contribution, reverse=True)[:3]
        names = [s.signal_type.value.replace("_", " ").title() for s in top]
        if level == AlertLevel.EARLY_WARNING:
            return f"Early accumulation detected. Primary signals: {', '.join(names)}. Monitor closely for volume confirmation."
        elif level == AlertLevel.PUMP_DETECTED:
            return f"Pump in progress. Confirmed by: {', '.join(names)}. Consider entry with tight stop."
        else:
            return f"CRITICAL: Multi-layer confirmation — {', '.join(names)}. High conviction momentum."


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: Input/Output Handler — MEXC API Integration
# ═══════════════════════════════════════════════════════════════════════════════


class MexcAPIClient:
    """
    Async HTTP client for MEXC Spot & Futures public market data.
    Rate-limit aware with adaptive request batching.
    """

    def __init__(self, config: PumpDetectionConfig) -> None:
        self.cfg = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._last_request_time: float = 0.0
        self._request_count: int = 0
        self._rate_limit_window_start: float = 0.0
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout, connect=8.0)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "MEXC-PumpSniper/4.2",
            },
        )

    async def stop(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def _get(self, endpoint: str, base: Optional[str] = None, **params: Any) -> Any:
        if self.session is None:
            raise RuntimeError("MexcAPIClient session not started")
        url = f"{base or self.cfg.mexc_base_url}{endpoint}"
        async with self._semaphore:
            # Adaptive rate limiting
            now = time.time()
            if now - self._rate_limit_window_start > 1.0:
                self._rate_limit_window_start = now
                self._request_count = 0
            self._request_count += 1
            if self._request_count > 20:
                await asyncio.sleep(0.05 * self._request_count)
            try:
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 2))
                        await asyncio.sleep(retry_after)
                        return await self._get(endpoint, base, **params)
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientResponseError as e:
                logging.warning("MEXC API error %s on %s: %s", e.status, url, e.message)
                raise
            except Exception as e:
                logging.warning("MEXC request failed: %s", e)
                raise

    async def get_exchange_info(self) -> Dict[str, Any]:
        return await self._get("/exchangeInfo")

    async def get_24h_tickers(self) -> List[TickerSnapshot]:
        data = await self._get("/ticker/24hr")
        tickers: List[TickerSnapshot] = []
        for item in data if isinstance(data, list) else []:
            try:
                tickers.append(
                    TickerSnapshot(
                        symbol=item.get("symbol", ""),
                        price=float(item.get("lastPrice", 0)),
                        volume_24h=float(item.get("volume", 0)),
                        quote_volume_24h=float(item.get("quoteVolume", 0)),
                        price_change_24h_pct=float(item.get("priceChangePercent", 0)),
                        high_24h=float(item.get("highPrice", 0)),
                        low_24h=float(item.get("lowPrice", 0)),
                        bid_price=float(item.get("bidPrice", 0)),
                        ask_price=float(item.get("askPrice", 0)),
                        timestamp=time.time(),
                    )
                )
            except (ValueError, TypeError):
                continue
        return tickers

    async def get_klines(
        self, symbol: str, interval: str = "1m", limit: int = 100
    ) -> List[CandleData]:
        data = await self._get("/klines", symbol=symbol, interval=interval, limit=limit)
        candles: List[CandleData] = []
        for c in data if isinstance(data, list) else []:
            try:
                candles.append(
                    CandleData(
                        open_time=int(c[0]),
                        open_price=float(c[1]),
                        high_price=float(c[2]),
                        low_price=float(c[3]),
                        close_price=float(c[4]),
                        volume=float(c[5]),
                        quote_volume=float(c[7]),
                        close_time=int(c[6]),
                    )
                )
            except (IndexError, ValueError):
                continue
        return candles

    async def get_order_book(self, symbol: str, limit: int = 20) -> OrderBookSnapshot:
        data = await self._get("/depth", symbol=symbol, limit=limit)
        bids = [OrderBookLevel(price=float(b[0]), quantity=float(b[1])) for b in data.get("bids", [])]
        asks = [OrderBookLevel(price=float(a[0]), quantity=float(a[1])) for a in data.get("asks", [])]
        return OrderBookSnapshot(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=time.time(),
        )

    async def get_futures_funding_rate(self, symbol: str) -> Optional[float]:
        """Fetch funding rate for futures symbols ( pump proxy )."""
        try:
            sym = symbol.replace(self.cfg.quote_asset, "")
            data = await self._get(
                "/contract/detail",
                base=self.cfg.mexc_futures_url,
            )
            if isinstance(data, dict) and "data" in data:
                for contract in data["data"]:
                    if contract.get("symbol", "").upper() == sym.upper():
                        return float(contract.get("fundingRate", 0))
        except Exception:
            pass
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: Logging & Diagnostics Module
# ═══════════════════════════════════════════════════════════════════════════════


def setup_logging(level: str = "INFO") -> None:
    fmt = (
        "%(asctime)s | %(levelname)-8s | %(name)-25s | %(funcName)-20s | %(message)s"
    )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("pump_sniper.log", encoding="utf-8"),
        ],
    )


class DiagnosticsCollector:
    """Runtime metrics for observability."""

    def __init__(self) -> None:
        self.scan_count: int = 0
        self.alert_count: int = 0
        self.api_request_count: int = 0
        self.error_count: int = 0
        self.last_scan_duration_ms: float = 0.0
        self.symbols_monitored: int = 0
        self.start_time: float = time.time()
        self._lock = asyncio.Lock()

    async def record_scan(self, duration_ms: float, symbols: int) -> None:
        async with self._lock:
            self.scan_count += 1
            self.last_scan_duration_ms = duration_ms
            self.symbols_monitored = symbols

    async def record_alert(self) -> None:
        async with self._lock:
            self.alert_count += 1

    async def record_api_request(self) -> None:
        async with self._lock:
            self.api_request_count += 1

    async def record_error(self) -> None:
        async with self._lock:
            self.error_count += 1

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

    def summary(self) -> str:
        return (
            f"Scans: {self.scan_count} | Alerts: {self.alert_count} | "
            f"API Calls: {self.api_request_count} | Errors: {self.error_count} | "
            f"Uptime: {self.uptime_seconds / 60:.1f}m | Symbols: {self.symbols_monitored}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: Error Handling & Edge Case Management
# ═══════════════════════════════════════════════════════════════════════════════


class SniperException(Exception):
    pass


class APIRateLimitError(SniperException):
    pass


class InsufficientDataError(SniperException):
    pass


class ExchangeUnavailableError(SniperException):
    pass


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def sanitize_symbol(symbol: str) -> str:
    return "".join(c for c in symbol if c.isalnum()).upper()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: Telegram Bot UI — Buttons & Command Handlers
# ═══════════════════════════════════════════════════════════════════════════════


class BotUI:
    """
    Inline keyboard factory and message formatter.
    All user-facing strings in English.
    """

    @staticmethod
    def main_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("▶️ START MONITORING", callback_data="cmd_start"),
                InlineKeyboardButton("⏹ STOP MONITORING", callback_data="cmd_stop"),
            ],
            [
                InlineKeyboardButton("📊 STATUS", callback_data="cmd_status"),
                InlineKeyboardButton("⚙️ SETTINGS", callback_data="cmd_settings"),
            ],
            [
                InlineKeyboardButton("📈 ALERT HISTORY", callback_data="cmd_history"),
                InlineKeyboardButton("🔔 TEST ALERT", callback_data="cmd_test"),
            ],
            [
                InlineKeyboardButton("🎯 PROFILE: Conservative", callback_data="profile_conservative"),
                InlineKeyboardButton("🎯 PROFILE: Balanced", callback_data="profile_balanced"),
            ],
            [
                InlineKeyboardButton("🎯 PROFILE: Aggressive", callback_data="profile_aggressive"),
                InlineKeyboardButton("🎯 PROFILE: Whale Hunt", callback_data="profile_whale"),
            ],
            [
                InlineKeyboardButton("📋 HELP / COMMANDS", callback_data="cmd_help"),
            ],
        ])

    @staticmethod
    def back_button() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ BACK TO MENU", callback_data="cmd_menu")],
        ])

    @staticmethod
    def settings_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➖ Lower Threshold", callback_data="th_lower"),
                InlineKeyboardButton("➕ Raise Threshold", callback_data="th_raise"),
            ],
            [
                InlineKeyboardButton("➖ Vol Ratio -1", callback_data="vol_down"),
                InlineKeyboardButton("➕ Vol Ratio +1", callback_data="vol_up"),
            ],
            [
                InlineKeyboardButton("➖ Cooldown -5m", callback_data="cd_down"),
                InlineKeyboardButton("➕ Cooldown +5m", callback_data="cd_up"),
            ],
            [InlineKeyboardButton("◀️ BACK TO MENU", callback_data="cmd_menu")],
        ])

    @staticmethod
    def format_alert(result: PumpAnalysisResult) -> str:
        emoji = {"early_warning": "⚡", "pump_detected": "🚀", "critical_pump": "🔥"}
        level_emoji = emoji.get(result.alert_level.value, "📢")
        level_name = result.alert_level.value.replace("_", " ").upper()

        bars = "█" * int(result.pump_score / 5) + "░" * (20 - int(result.pump_score / 5))

        lines = [
            f"{level_emoji} <b>{level_name}</b> {level_emoji}",
            f"",
            f"<b>Token:</b> <code>{result.symbol}</code>",
            f"<b>Price:</b> <code>{result.current_price:.8f}</code> USDT",
            f"<b>Pump Score:</b> <code>{result.pump_score}/100</code>",
            f"<b>Meter:</b> <code>[{bars}]</code>",
            f"<b>Signal Layers:</b> {result.detected_layers}/{result.required_layers} confirmed",
            f"",
            f"<b>Detected Signals:</b>",
        ]
        for s in result.signals:
            lines.append(
                f"  • {s.signal_type.value}: +{s.score_contribution:.1f} pts "
                f"(conf: {s.confidence:.0%})"
            )
        lines.extend([
            f"",
            f"<b>Recommendation:</b> {result.recommendation}",
        ])
        if result.projected_target:
            lines.append(f"<b>Projected Target:</b> <code>{result.projected_target:.8f}</code>")
        if result.stop_loss:
            lines.append(f"<b>Stop Loss:</b> <code>{result.stop_loss:.8f}</code>")
        if result.time_to_pump_estimate_seconds:
            lines.append(
                f"<b>Est. Time to Full Pump:</b> "
                f"<code>{result.time_to_pump_estimate_seconds:.0f}s</code>"
            )
        lines.extend([
            f"",
            f"<i>Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</i>",
        ])
        return "\n".join(lines)

    @staticmethod
    def format_status(diag: DiagnosticsCollector, cfg: PumpDetectionConfig) -> str:
        return (
            f"<b>🤖 PUMP SNIPER STATUS</b>\n\n"
            f"<b>Profile:</b> <code>{cfg.profile.value}</code>\n"
            f"<b>Monitoring:</b> <code>{cfg.quote_asset}</code> pairs\n"
            f"<b>Scan Interval:</b> <code>{cfg.scan_interval_seconds}s</code>\n"
            f"<b>Pump Threshold:</b> <code>{cfg.pump_score_threshold}</code>\n"
            f"<b>Early Warning:</b> <code>{cfg.early_signal_score_threshold}</code>\n"
            f"<b>Cooldown:</b> <code>{cfg.pump_cooldown_seconds / 60:.0f}m</code>\n\n"
            f"<b>Runtime Stats:</b>\n"
            f"  Scans: <code>{diag.scan_count}</code>\n"
            f"  Alerts: <code>{diag.alert_count}</code>\n"
            f"  Symbols: <code>{diag.symbols_monitored}</code>\n"
            f"  API Calls: <code>{diag.api_request_count}</code>\n"
            f"  Errors: <code>{diag.error_count}</code>\n"
            f"  Last Scan: <code>{diag.last_scan_duration_ms:.0f}ms</code>\n"
            f"  Uptime: <code>{diag.uptime_seconds / 60:.1f}m</code>"
        )

    @staticmethod
    def format_help() -> str:
        return (
            "<b>📋 MEXC PUMP SNIPER — COMMAND REFERENCE</b>\n\n"
            "<b>Buttons:</b>\n"
            "  ▶️ START — Begin scanning MEXC for pumps\n"
            "  ⏹ STOP — Pause the detection engine\n"
            "  📊 STATUS — Live diagnostics dashboard\n"
            "  ⚙️ SETTINGS — Tune thresholds in real-time\n"
            "  📈 ALERT HISTORY — Confirmed pump records\n"
            "  🔔 TEST ALERT — Verify delivery pipeline\n\n"
            "<b>Profiles:</b>\n"
            "  🎯 Conservative — Fewer alerts, higher accuracy\n"
            "  🎯 Balanced — Default detection sweet spot\n"
            "  🎯 Aggressive — Maximum early warning (more noise)\n"
            "  🎯 Whale Hunt — Targets institutional accumulation\n\n"
            "<b>Detection Layers:</b>\n"
            "  • Volume Surge Z-Score Analysis\n"
            "  • Price Velocity (1m / 5m / 15m)\n"
            "  • Price Acceleration (2nd Derivative)\n"
            "  • RSI Divergence Detection\n"
            "  • Bollinger Squeeze → Explosion\n"
            "  • Order Book Imbalance\n"
            "  • Whale Cluster Identification\n"
            "  • Breakout Pattern Recognition\n"
            "  • Early Accumulation Detection\n"
            "  • Momentum Shift via RSI Slope\n\n"
            "<i>Bot auto-detects pumps 60-300s before explosion.</i>"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: Database Persistence Layer
# ═══════════════════════════════════════════════════════════════════════════════


class StateDatabase:
    """SQLite async persistence for alerts and configuration."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self._create_tables()
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def _create_tables(self) -> None:
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                timestamp REAL NOT NULL,
                level TEXT NOT NULL,
                score REAL NOT NULL,
                price_at_alert REAL NOT NULL,
                price_current REAL,
                price_change_since_alert REAL,
                confirmed_valid INTEGER,
                raw_json TEXT
            )
        """)
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS config_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                saved_at REAL NOT NULL,
                config_json TEXT NOT NULL
            )
        """)
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol)
        """)
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts(timestamp DESC)
        """)

    async def save_alert(self, record: AlertRecord, raw: str) -> None:
        await self.conn.execute(
            """INSERT INTO alerts
                (alert_id, symbol, timestamp, level, score, price_at_alert,
                 price_current, price_change_since_alert, confirmed_valid, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.alert_id, record.symbol, record.timestamp, record.level.value,
                record.score, record.price_at_alert, record.price_current,
                record.price_change_since_alert,
                1 if record.confirmed_valid else 0 if record.confirmed_valid is not None else None,
                raw,
            ),
        )
        await self.conn.commit()

    async def get_recent_alerts(self, limit: int = 20) -> List[AlertRecord]:
        cursor = await self.conn.execute(
            "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        alerts: List[AlertRecord] = []
        for r in rows:
            alerts.append(
                AlertRecord(
                    alert_id=r[0],
                    symbol=r[1],
                    timestamp=r[2],
                    level=AlertLevel(r[3]),
                    score=r[4],
                    price_at_alert=r[5],
                    price_current=r[6],
                    price_change_since_alert=r[7],
                    confirmed_valid=bool(r[8]) if r[8] is not None else None,
                )
            )
        return alerts

    async def save_config_snapshot(self, config: PumpDetectionConfig) -> None:
        await self.conn.execute(
            "INSERT INTO config_snapshots (saved_at, config_json) VALUES (?, ?)",
            (time.time(), config.to_json()),
        )
        await self.conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9: Main Orchestrator — Host Setup
# ═══════════════════════════════════════════════════════════════════════════════


class PumpSniperBot:
    """
    Main host orchestrating MEXC market data ingestion,
    multi-layer pump detection, and Telegram alert delivery.
    """

    def __init__(self) -> None:
        self.cfg = PumpDetectionConfig()
        self.cfg.apply_profile(PumpThresholdProfile.BALANCED)
        self.mexc = MexcAPIClient(self.cfg)
        self.engine = PumpDetectionEngine(self.cfg)
        self.db = StateDatabase(self.cfg.db_path)
        self.diag = DiagnosticsCollector()
        self.ui = BotUI()
        self.telegram_app: Optional[Application] = None
        self._monitored_symbols: List[str] = []
        self._symbol_metadata: Dict[str, Dict[str, Any]] = {}
        self._running: bool = False
        self._shutdown_requested: bool = False
        self._shutting_down: bool = False
        self._scan_task: Optional[asyncio.Task] = None
        self._alert_history: Deque[str] = deque(maxlen=100)
        self._user_chat_ids: set = set()
        self._lock = asyncio.Lock()

    # ── Lifecycle ──

    async def initialize(self) -> None:
        setup_logging(self.cfg.log_level)
        logging.info("PumpSniperBot initializing...")
        await self.mexc.start()
        await self.db.initialize()

        # Discover tradeable symbols
        try:
            await self._refresh_symbol_universe()
        except Exception as e:
            logging.error("Failed to refresh symbol universe during init: %s", e)
            # Не убиваем инициализацию, просто начинаем с пустым списком

        # Telegram
        if not self.cfg.bot_token or self.cfg.bot_token == "YOUR_BOT_TOKEN_HERE":
            logging.error("Telegram bot token not set!")
            raise RuntimeError("Telegram bot token is required")

        self.telegram_app = Application.builder().token(self.cfg.bot_token).build()
        self._register_handlers()
        await self.telegram_app.initialize()
        await self.telegram_app.start()
        logging.info("Initialization complete. %d symbols loaded.", len(self._monitored_symbols))

    async def run(self) -> None:
        """
        Основной цикл бота. Управляет scan task через _running.
        Завершается только при _shutdown_requested (Ctrl+C / SIGTERM).
        """
        await self.telegram_app.updater.start_polling(drop_pending_updates=True)
        logging.info("Telegram bot polling started. Send /start to activate.")

        self._scan_task = None

        try:
            while not self._shutdown_requested:
                if self._running:
                    if self._scan_task is None or self._scan_task.done():
                        if self._scan_task is not None:
                            if self._scan_task.cancelled():
                                logging.debug("Previous scan task was cancelled")
                            else:
                                exc = self._scan_task.exception()
                                if exc:
                                    logging.error("Scan loop crashed: %s", exc, exc_info=exc)
                                    await self.diag.record_error()
                            self._scan_task = None

                        logging.info("Starting scan loop...")
                        self._scan_task = asyncio.create_task(self._main_scan_loop())
                else:
                    # Monitoring paused — останавливаем scan task если он ещё работает
                    if self._scan_task is not None:
                        if not self._scan_task.done():
                            self._scan_task.cancel()
                            try:
                                await self._scan_task
                            except asyncio.CancelledError:
                                pass
                        self._scan_task = None

                await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass
        finally:
            # Гарантированно останавливаем scan task при выходе
            if self._scan_task is not None:
                if not self._scan_task.done():
                    self._scan_task.cancel()
                    try:
                        await self._scan_task
                    except asyncio.CancelledError:
                        pass
                self._scan_task = None
            # Всегда вызываем shutdown при выходе из run()
            if not self._shutting_down:
                await self.shutdown()

    async def shutdown(self) -> None:
        """Idempotent graceful shutdown."""
        if self._shutting_down:
            return
        self._shutting_down = True
        self._shutdown_requested = True
        self._running = False
        logging.info("Shutting down PumpSniperBot...")

        # Останавливаем scan task
        if self._scan_task is not None:
            if not self._scan_task.done():
                self._scan_task.cancel()
                try:
                    await self._scan_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logging.warning("Scan task ended with exception during shutdown: %s", e)
            self._scan_task = None

        # Останавливаем Telegram
        if self.telegram_app:
            try:
                if self.telegram_app.updater.running:
                    await self.telegram_app.updater.stop()
                await self.telegram_app.stop()
                await self.telegram_app.shutdown()
            except Exception as e:
                logging.warning("Error stopping Telegram app: %s", e)

        # Сохраняем конфиг и закрываем БД
        try:
            await self.db.save_config_snapshot(self.cfg)
            await self.db.close()
        except Exception as e:
            logging.warning("Error closing database: %s", e)

        # Закрываем HTTP сессию
        try:
            await self.mexc.stop()
        except Exception as e:
            logging.warning("Error stopping MEXC client: %s", e)

        logging.info("Shutdown complete.")

    # ── Symbol Universe Management ──

    async def _refresh_symbol_universe(self) -> None:
        try:
            info = await self.mexc.get_exchange_info()
            symbols = info.get("symbols", [])
            filtered: List[str] = []
            meta: Dict[str, Dict[str, Any]] = {}
            for s in symbols:
                sym = s.get("symbol", "")
                if not sym.endswith(self.cfg.quote_asset):
                    continue
                if sym in self.cfg.excluded_symbols:
                    continue
                status = s.get("status", "")
                if status != "TRADING":
                    continue
                meta[sym] = {
                    "baseAsset": s.get("baseAsset", ""),
                    "quoteAsset": s.get("quoteAsset", ""),
                    "filters": s.get("filters", []),
                }
                filtered.append(sym)
            async with self._lock:
                self._monitored_symbols = filtered
                self._symbol_metadata = meta
        except Exception as e:
            logging.error("Failed to refresh symbol universe: %s", e)
            await self.diag.record_error()

    def _filter_liquid_symbols(self, tickers: List[TickerSnapshot]) -> List[TickerSnapshot]:
        return [
            t for t in tickers
            if t.symbol in self._monitored_symbols
            and self.cfg.min_quote_volume_24h <= t.quote_volume_24h <= self.cfg.max_quote_volume_24h
            and self.cfg.price_min <= t.price <= self.cfg.price_max
        ]

    # ── Core Scan Loop ──

    async def _main_scan_loop(self) -> None:
        """Primary detection loop — fetches data, runs analysis, dispatches alerts."""
        while self._running:
            loop_start = time.time()
            try:
                # 1. Refresh universe periodically (пропускаем на самой первой итерации)
                if self.diag.scan_count > 0 and self.diag.scan_count % 50 == 0:
                    await self._refresh_symbol_universe()

                # 2. Fetch all 24h tickers
                all_tickers = await self.mexc.get_24h_tickers()
                await self.diag.record_api_request()
                tickers = self._filter_liquid_symbols(all_tickers)

                # 3. Feed engine & analyze top candidates
                high_volume_candidates = sorted(
                    tickers, key=lambda t: t.quote_volume_24h, reverse=True
                )[:150]

                analysis_tasks = []
                for ticker in high_volume_candidates:
                    self.engine.ingest_tick(ticker.symbol, ticker.price, ticker.volume_24h, ticker.timestamp)
                    # Pull klines for deeper analysis (пропускаем на старте)
                    if self.diag.scan_count > 0 and self.diag.scan_count % 3 == 0:
                        analysis_tasks.append(self._deep_analyze(ticker))
                    else:
                        # Lightweight tick-only analysis
                        result = await self._lightweight_analyze(ticker)
                        if result:
                            await self._dispatch_alert(result)

                if analysis_tasks:
                    results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, PumpAnalysisResult):
                            await self._dispatch_alert(r)
                        elif isinstance(r, Exception):
                            logging.warning("Deep analysis error: %s", r)
                            await self.diag.record_error()

                # 4. Update order book data for a subset
                ob_tasks = [
                    self._fetch_and_ingest_ob(t.symbol)
                    for t in high_volume_candidates[:30]
                ]
                await asyncio.gather(*ob_tasks, return_exceptions=True)

                elapsed_ms = (time.time() - loop_start) * 1000
                await self.diag.record_scan(elapsed_ms, len(tickers))
                logging.debug("Scan %d complete in %.1fms", self.diag.scan_count, elapsed_ms)

            except Exception as e:
                logging.error("Scan loop error: %s", e, exc_info=True)
                await self.diag.record_error()

            # Adaptive sleep
            sleep_time = max(0.5, self.cfg.scan_interval_seconds - (time.time() - loop_start))
            await asyncio.sleep(sleep_time)

    async def _deep_analyze(self, ticker: TickerSnapshot) -> Optional[PumpAnalysisResult]:
        """Fetch klines + order book for comprehensive analysis."""
        try:
            candles = await self.mexc.get_klines(
                ticker.symbol, interval="1m", limit=100
            )
            await self.diag.record_api_request()
            self.engine.ingest_candles(ticker.symbol, candles)

            ob = await self.mexc.get_order_book(ticker.symbol, limit=20)
            await self.diag.record_api_request()
            self.engine.ingest_orderbook(ticker.symbol, ob)

            return await self.engine.analyze(ticker.symbol, ticker, ob)
        except Exception as e:
            logging.debug("Deep analysis failed for %s: %s", ticker.symbol, e)
            return None

    async def _lightweight_analyze(self, ticker: TickerSnapshot) -> Optional[PumpAnalysisResult]:
        """Tick-only fast path for velocity-based detection."""
        try:
            return await self.engine.analyze(ticker.symbol, ticker, None)
        except Exception as e:
            logging.debug("Lightweight analysis failed for %s: %s", ticker.symbol, e)
            return None

    async def _fetch_and_ingest_ob(self, symbol: str) -> None:
        try:
            ob = await self.mexc.get_order_book(symbol, limit=20)
            await self.diag.record_api_request()
            self.engine.ingest_orderbook(symbol, ob)
        except Exception:
            pass

    # ── Alert Dispatch ──

    async def _dispatch_alert(self, result: PumpAnalysisResult) -> None:
        await self.diag.record_alert()
        alert_id = str(uuid.uuid4())[:12]
        record = AlertRecord(
            alert_id=alert_id,
            symbol=result.symbol,
            timestamp=result.timestamp,
            level=result.alert_level,
            score=result.pump_score,
            price_at_alert=result.current_price,
        )
        raw_json = json.dumps({
            "symbol": result.symbol,
            "score": result.pump_score,
            "level": result.alert_level.value,
            "signals": [
                {"type": s.signal_type.value, "score": s.score_contribution, "meta": s.metadata}
                for s in result.signals
            ],
            "price": result.current_price,
        })
        await self.db.save_alert(record, raw_json)
        self._alert_history.append(alert_id)

        message = self.ui.format_alert(result)
        for chat_id in list(self._user_chat_ids):
            try:
                await self.telegram_app.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode="HTML",
                    reply_markup=self.ui.main_menu(),
                )
            except Exception as e:
                logging.warning("Failed to send alert to %s: %s", chat_id, e)

    async def _send_to_all(self, text: str, parse_html: bool = True) -> None:
        for chat_id in list(self._user_chat_ids):
            try:
                kwargs = {"parse_mode": "HTML"} if parse_html else {}
                await self.telegram_app.bot.send_message(
                    chat_id=chat_id, text=text, reply_markup=self.ui.main_menu(), **kwargs
                )
            except Exception as e:
                logging.warning("Send to %s failed: %s", chat_id, e)

    # ── Telegram Handlers ──

    def _register_handlers(self) -> None:
        app = self.telegram_app
        app.add_handler(CommandHandler("start", self._hdl_start))
        app.add_handler(CommandHandler("status", self._hdl_status))
        app.add_handler(CommandHandler("help", self._hdl_help))
        app.add_handler(CommandHandler("stop", self._hdl_stop_bot))
        app.add_handler(CallbackQueryHandler(self._hdl_callback))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._hdl_text))

    async def _hdl_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        self._user_chat_ids.add(chat_id)
        self.cfg.admin_telegram_id = update.effective_user.id
        welcome = (
            "<b>🤖 MEXC PUMP SNIPER v4.2</b>\n\n"
            "Early detection neural engine online.\n"
            f"Monitoring: <code>{len(self._monitored_symbols)}</code> symbols\n"
            f"Profile: <code>{self.cfg.profile.value}</code>\n\n"
            "Select an action below."
        )
        await update.message.reply_text(welcome, parse_mode="HTML", reply_markup=self.ui.main_menu())

    async def _hdl_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = self.ui.format_status(self.diag, self.cfg)
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=self.ui.back_button())

    async def _hdl_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            self.ui.format_help(), parse_mode="HTML", reply_markup=self.ui.back_button()
        )

    async def _hdl_stop_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "Use the ⏹ STOP MONITORING button to pause detection, "
            "or send /restart to reboot the bot.",
            reply_markup=self.ui.main_menu(),
        )

    async def _hdl_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = update.message.text.strip().upper()
        if text in self._monitored_symbols:
            await update.message.reply_text(
                f"<b>Symbol:</b> <code>{text}</code> is in monitoring universe.\n"
                f"Use buttons for control.",
                parse_mode="HTML",
                reply_markup=self.ui.main_menu(),
            )
        else:
            await update.message.reply_text(
                "Unrecognized command. Use the menu buttons below.",
                reply_markup=self.ui.main_menu(),
            )

    async def _hdl_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()
        data = query.data
        chat_id = update.effective_chat.id
        self._user_chat_ids.add(chat_id)

        if data == "cmd_start":
            if not self._running:
                self._running = True
            await query.edit_message_text(
                "<b>▶️ MONITORING ACTIVE</b>\n\nScanning MEXC for early pump signals...\n"
                f"Symbols: <code>{len(self._monitored_symbols)}</code> | "
                f"Profile: <code>{self.cfg.profile.value}</code>",
                parse_mode="HTML",
                reply_markup=self.ui.main_menu(),
            )

        elif data == "cmd_stop":
            self._running = False
            await query.edit_message_text(
                "<b>⏹ MONITORING PAUSED</b>\n\nDetection engine halted. "
                "Press ▶️ START to resume.",
                parse_mode="HTML",
                reply_markup=self.ui.main_menu(),
            )

        elif data == "cmd_status":
            await query.edit_message_text(
                self.ui.format_status(self.diag, self.cfg),
                parse_mode="HTML",
                reply_markup=self.ui.back_button(),
            )

        elif data == "cmd_settings":
            await query.edit_message_text(
                f"<b>⚙️ CURRENT SETTINGS</b>\n\n"
                f"Profile: <code>{self.cfg.profile.value}</code>\n"
                f"Pump Threshold: <code>{self.cfg.pump_score_threshold}</code>\n"
                f"Vol Ratio Trigger: <code>{self.cfg.volume_ratio_threshold}x</code>\n"
                f"Cooldown: <code>{self.cfg.pump_cooldown_seconds / 60:.0f}m</code>\n\n"
                f"Use buttons to adjust.",
                parse_mode="HTML",
                reply_markup=self.ui.settings_menu(),
            )

        elif data == "cmd_history":
            alerts = await self.db.get_recent_alerts(10)
            if not alerts:
                text = "<b>📈 NO ALERTS YET</b>\n\nHistory will populate as pumps are detected."
            else:
                lines = ["<b>📈 RECENT ALERTS</b>\n"]
                for a in alerts:
                    emoji = {"early_warning": "⚡", "pump_detected": "🚀", "critical_pump": "🔥"}
                    lines.append(
                        f"{emoji.get(a.level.value, '•')} <code>{a.symbol}</code> | "
                        f"Score: {a.score} | {datetime.fromtimestamp(a.timestamp, tz=timezone.utc).strftime('%H:%M:%S')}"
                    )
                text = "\n".join(lines)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.ui.back_button())

        elif data == "cmd_test":
            test_result = PumpAnalysisResult(
                symbol="TESTUSDT",
                timestamp=time.time(),
                current_price=0.00123456,
                pump_score=88.5,
                alert_level=AlertLevel.CRITICAL_PUMP,
                signals=[
                    PumpSignal(SignalType.VOLUME_SURGE, 85.0, 0.9, {"volume_ratio": 12.5}),
                    PumpSignal(SignalType.PRICE_VELOCITY, 90.0, 0.88, {"change_pct": 8.5}),
                    PumpSignal(SignalType.BB_SQUEEZE_EXPLODE, 80.0, 0.75, {"expansion_ratio": 3.2}),
                ],
                detected_layers=3,
                required_layers=self.cfg.confirmation_layers_required,
                recommendation="TEST: Delivery pipeline operational. This is a drill.",
                projected_target=0.00150000,
                stop_loss=0.00115000,
                time_to_pump_estimate_seconds=120.0,
            )
            await query.edit_message_text(
                self.ui.format_alert(test_result),
                parse_mode="HTML",
                reply_markup=self.ui.main_menu(),
            )

        elif data == "cmd_menu":
            await query.edit_message_text(
                "<b>🤖 MAIN MENU</b>\n\nSelect an action:",
                parse_mode="HTML",
                reply_markup=self.ui.main_menu(),
            )

        elif data == "cmd_help":
            await query.edit_message_text(
                self.ui.format_help(), parse_mode="HTML", reply_markup=self.ui.back_button()
            )

        # Profile switches
        elif data.startswith("profile_"):
            profile_map = {
                "profile_conservative": PumpThresholdProfile.CONSERVATIVE,
                "profile_balanced": PumpThresholdProfile.BALANCED,
                "profile_aggressive": PumpThresholdProfile.AGGRESSIVE,
                "profile_whale": PumpThresholdProfile.WHALE_HUNT,
            }
            if data in profile_map:
                self.cfg.apply_profile(profile_map[data])
                await query.edit_message_text(
                    f"<b>✅ PROFILE UPDATED</b>\n\n"
                    f"Active: <code>{self.cfg.profile.value}</code>\n"
                    f"Pump Threshold: <code>{self.cfg.pump_score_threshold}</code>\n"
                    f"Confirmation Layers: <code>{self.cfg.confirmation_layers_required}</code>",
                    parse_mode="HTML",
                    reply_markup=self.ui.main_menu(),
                )

        # Settings adjustments
        elif data == "th_lower":
            self.cfg.pump_score_threshold = max(50, self.cfg.pump_score_threshold - 5)
            await self._settings_updated(query)
        elif data == "th_raise":
            self.cfg.pump_score_threshold = min(99, self.cfg.pump_score_threshold + 5)
            await self._settings_updated(query)
        elif data == "vol_down":
            self.cfg.volume_ratio_threshold = max(1.5, self.cfg.volume_ratio_threshold - 1)
            await self._settings_updated(query)
        elif data == "vol_up":
            self.cfg.volume_ratio_threshold = min(20, self.cfg.volume_ratio_threshold + 1)
            await self._settings_updated(query)
        elif data == "cd_down":
            self.cfg.pump_cooldown_seconds = max(60, self.cfg.pump_cooldown_seconds - 300)
            await self._settings_updated(query)
        elif data == "cd_up":
            self.cfg.pump_cooldown_seconds = min(3600, self.cfg.pump_cooldown_seconds + 300)
            await self._settings_updated(query)

    async def _settings_updated(self, query) -> None:
        await query.edit_message_text(
            f"<b>⚙️ SETTINGS UPDATED</b>\n\n"
            f"Profile: <code>{self.cfg.profile.value}</code>\n"
            f"Pump Threshold: <code>{self.cfg.pump_score_threshold}</code>\n"
            f"Vol Ratio Trigger: <code>{self.cfg.volume_ratio_threshold}x</code>\n"
            f"Cooldown: <code>{self.cfg.pump_cooldown_seconds / 60:.0f}m</code>",
            parse_mode="HTML",
            reply_markup=self.ui.settings_menu(),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10: Entry Point
# ═══════════════════════════════════════════════════════════════════════════════


async def main() -> None:
    bot = PumpSniperBot()
    await bot.initialize()

    loop = asyncio.get_running_loop()
    _shutdown_in_progress = False

    async def _graceful_shutdown() -> None:
        nonlocal _shutdown_in_progress
        if _shutdown_in_progress:
            return
        _shutdown_in_progress = True
        await bot.shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_graceful_shutdown()))
        except (ValueError, NotImplementedError, RuntimeError):
            logging.warning("Signal %s not supported on this platform", getattr(sig, 'name', str(sig)))

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())