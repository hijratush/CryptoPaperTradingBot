# 🤖 Crypto Paper Trading Bot — SPOT USDT Only

Simulasi live paper trading crypto yang terhubung ke WebSocket Tokocrypto secara real-time.
Semua order adalah simulasi — tidak ada uang nyata yang digunakan.

---

## 🏗️ Arsitektur Proyek

```
crypto_bot/
├── main.py                      ← Entrypoint utama
├── requirements.txt
├── .env.example
│
├── src/
│   ├── core/
│   │   ├── config.py            ← Settings (pydantic-settings)
│   │   ├── models.py            ← Domain models: Tick, Order, Portfolio, Position
│   │   ├── engine.py            ← Paper Trading Engine (order execution + fee calc)
│   │   ├── event_bus.py         ← Async pub/sub event bus
│   │   └── logging_setup.py     ← Centralized logging (rich + rotating file)
│   │
│   ├── data/
│   │   ├── feed.py              ← Tokocrypto WebSocket client (reconnect, stale detection)
│   │   └── price_store.py       ← In-memory price store + tick history + JSONL persistence
│   │
│   ├── strategy/
│   │   └── ema_cross.py         ← EMA Crossover + RSI strategy + SL/TP
│   │
│   └── dashboard/
│       └── app.py               ← FastAPI dashboard (REST + WebSocket push)
│
├── data/                        ← Auto-created: trade logs, price logs, snapshots
│   ├── trades.jsonl             ← Every trade, append-only
│   ├── prices.jsonl             ← Tick data stream
│   └── portfolio_snapshots.jsonl ← Portfolio state every 60 seconds
│
└── logs/
    └── trading_bot.log          ← Rotating log file (10 MB × 5 backups)
```

---

## 🔄 Alur Data (Event-Driven Architecture)

```
Tokocrypto WS ──► TokocryptoFeed
                       │
                       ▼ publish(Topics.TICK)
                   EventBus
                  /         \
          PriceStore       EMACrossStrategy
          (persist)        (analyze + signal)
                               │
                    publish(Topics.SIGNAL_BUY/SELL)
                               │
                               ▼
                    PaperTradingEngine
                    (execute + calc fees)
                               │
                    publish(Topics.ORDER_FILLED)
                    publish(Topics.PORTFOLIO_UPDATE)
                               │
                               ▼
                    Dashboard (FastAPI WS push)
```

---

## 📊 Strategi Trading: EMA Crossover + RSI

| Parameter | Nilai |
|-----------|-------|
| EMA Fast  | 9 periode |
| EMA Slow  | 21 periode |
| RSI       | 14 periode |
| RSI Overbought | 70 |
| Max posisi per pair | 20% balance |
| Stop Loss | -3% dari avg buy |
| Take Profit | +5% dari avg buy |

**BUY**: EMA(9) crosses above EMA(21) AND RSI < 70
**SELL**: EMA(9) crosses below EMA(21) OR RSI > 70 OR SL/TP triggered

---

## 💰 Struktur Fee (Tokocrypto USDT Pairs)

| Komponen | Rate |
|----------|------|
| Base taker fee | 0.10% |
| VAT | 0.12% |
| CFT (USDT pairs) | 0.0444% |
| **Total per trade** | **~0.2644%** |

Fee dihitung secara realistis di setiap order.

---

## 🚀 Cara Menjalankan

### 1. Prasyarat

```bash
# Python 3.11+ diperlukan
python3 --version

# Install pip jika belum ada
sudo apt install python3-pip python3-venv
```

### 2. Setup Project

```bash
# Clone / buat folder project
cd ~/

# Buat virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Konfigurasi (Opsional)

```bash
# Copy contoh .env
cp .env.example .env

# Edit jika diperlukan (semua sudah ada default yang bagus)
nano .env
```

### 4. Jalankan Bot

```bash
# Aktifkan venv jika belum
source venv/bin/activate

# Jalankan
python main.py
```

### 5. Buka Dashboard

Buka browser dan akses:
```
http://localhost:8080
```

Dashboard menampilkan:
- 📈 Portfolio value real-time
- 💵 USDT balance + P&L
- 📊 Harga live semua pair
- 🔍 Indikator EMA + RSI per symbol
- 📋 Posisi terbuka dengan unrealized P&L
- 🔄 Riwayat trade terbaru

---

## 📁 File Output

Semua aktivitas disimpan otomatis di folder `data/`:

```
data/trades.jsonl              # Setiap trade (JSONL, append-only)
data/prices.jsonl              # Tick harga (volume besar)
data/portfolio_snapshots.jsonl # Snapshot portfolio tiap 60 detik
logs/trading_bot.log           # Log aplikasi (rotating)
```

### Contoh Format Trade Log:
```json
{"order_id":"a1b2c3d4","symbol":"BTCUSDT","side":"BUY","quantity":0.000123,
 "price":65432.10,"status":"FILLED","fee_usdt":0.0265,"net_usdt":-10.0,
 "timestamp":1710000000.0,"note":"EMA cross bullish | EMA9=65400 RSI=45.2"}
```

---

## 🛡️ Fitur Robustness

| Fitur | Detail |
|-------|--------|
| Auto-reconnect WS | Exponential backoff: 3s → 6s → 12s … max 60s |
| Stale detection | Alert jika tidak ada data >10 detik |
| Pong keepalive | Merespons ping server setiap 3 menit |
| 24h rotation | Koneksi WS Tokocrypto max 24 jam, otomatis reconnect |
| Async logging | Non-blocking, rotating 10MB × 5 backup |
| Error isolation | Handler error di satu subscriber tidak crash yang lain |
| Order validation | Cek balance, minimum order sebelum eksekusi |

---

## 🔧 Kustomisasi

Edit `src/core/config.py` untuk mengubah:
- `SYMBOLS`: tambah/kurang pair USDT
- `INITIAL_USDT_BALANCE`: modal awal simulasi
- `TAKER_FEE`, `EXTRA_FEE_USDT`: sesuaikan fee
- `WS_STALE_THRESHOLD`: threshold stale data
- `DASHBOARD_PORT`: port dashboard

Edit `src/strategy/ema_cross.py` untuk mengubah:
- `FAST_EMA`, `SLOW_EMA`: periode EMA
- `RSI_PERIOD`, `RSI_OVERBOUGHT`: parameter RSI
- `STOP_LOSS_PCT`, `TAKE_PROFIT_PCT`: risk management
- `MAX_POSITION_PCT`: ukuran posisi maksimal

---

## ⚠️ Disclaimer

Project ini **hanya untuk simulasi dan edukasi**.
Tidak ada koneksi ke akun exchange nyata.
Tidak ada uang nyata yang digunakan.
Past performance paper trading ≠ real trading results.
