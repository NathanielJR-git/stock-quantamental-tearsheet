import os
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots

# Configuration
API_URL = os.environ.get("API_URL", "http://backend:8000")

st.set_page_config(
    page_title="Stock Tearsheet",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        [data-testid="stMetric"] {
            background-color: rgba(255, 255, 255, 0.04);
            border: 1px solid rgba(255, 255, 255, 0.10);
            border-radius: 8px;
            padding: 14px 18px;
        }
        .block-container { padding-top: 1.5rem; }
        section[data-testid="stSidebar"] { min-width: 220px; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner="Loading tearsheet data…")
def fetch_tearsheet() -> list[dict]:
    try:
        resp = requests.get(f"{API_URL}/api/tearsheet-data", timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data", [])
    except requests.exceptions.ConnectionError:
        st.error(
            "Could not connect to the backend. "
            f"Make sure the server is running at **{API_URL}**.",
        )
        return []
    except requests.exceptions.HTTPError as exc:
        st.error(f"Backend returned an error: {exc}")
        return []
    except Exception as exc:  # noqa: BLE001
        st.error(f"Unexpected error fetching tearsheet: {exc}")
        return []


def fetch_chart_data(ticker: str) -> list[dict]:
    try:
        resp = requests.get(
            f"{API_URL}/api/chart-data/{ticker}", timeout=15
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data", [])
    except requests.exceptions.ConnectionError:
        st.warning(
            f"Could not reach backend to load chart data for **{ticker}**.",
        )
        return []
    except requests.exceptions.HTTPError as exc:
        st.warning(f"Backend error for chart data: {exc}")
        return []
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Unexpected error fetching chart data: {exc}")
        return []


_METRIC_LABELS: dict[str, str] = {
    "market_cap": "Market Cap",
    "ytd_return": "YTD Return",
    "high_52w": "52-Week High",
    "low_52w": "52-Week Low",
    "metric_1": "Metric 1",
    "metric_2": "Metric 2",
    "turnover": "Turnover",
    "optional_metric": "Optional Metric",
}

_INDICATOR_LABELS: dict[str, str] = {
    "indicator_1": "Indicator 1",
    "indicator_2": "Indicator 2",
    "indicator_3": "Indicator 3",
    "indicator_4": "Indicator 4",
}

_INDICATOR_COLORS: dict[str, str] = {
    "indicator_1": "#F6C90E",
    "indicator_2": "#3EC1D3",
    "indicator_3": "#FF6B6B",
    "indicator_4": "#B5EAD7",
}


def _is_blank(value) -> bool:
    if value is None:
        return True
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def _format_metric(key: str, value) -> str:
    if key == "ytd_return":
        return f"{float(value):.2f}%"
    if key in ("market_cap", "turnover"):
        v = float(value)
        if abs(v) >= 1e12:
            return f"${v / 1e12:.2f}T"
        if abs(v) >= 1e9:
            return f"${v / 1e9:.2f}B"
        if abs(v) >= 1e6:
            return f"${v / 1e6:.2f}M"
        return f"${v:,.0f}"
    if key in ("high_52w", "low_52w"):
        return f"${float(value):,.2f}"
    try:
        v = float(value)
        return f"{v:,.4f}" if abs(v) < 10 else f"{v:,.2f}"
    except (TypeError, ValueError):
        return str(value)


def _indicator_is_empty(series: pd.Series) -> bool:
    clean = series.dropna()
    if clean.empty:
        return True
    return (clean == 0).all()


def build_chart(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.8, 0.2],
        vertical_spacing=0.03,
        subplot_titles=("Price", "Volume"),
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["trading_date"],
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name="Price",
            increasing_line_color="#26A69A",
            decreasing_line_color="#EF5350",
            increasing_fillcolor="#26A69A",
            decreasing_fillcolor="#EF5350",
        ),
        row=1,
        col=1,
    )

    # Indicator overlays
    for col_key, label in _INDICATOR_LABELS.items():
        if col_key not in df.columns:
            continue
        if _indicator_is_empty(df[col_key]):
            continue
        fig.add_trace(
            go.Scatter(
                x=df["trading_date"],
                y=df[col_key],
                mode="lines",
                name=label,
                line=dict(color=_INDICATOR_COLORS[col_key], width=1.5),
            ),
            row=1,
            col=1,
        )

    # Volume bars
    bar_colors = [
        "#26A69A" if c >= o else "#EF5350"
        for c, o in zip(df["close_price"], df["open_price"])
    ]
    fig.add_trace(
        go.Bar(
            x=df["trading_date"],
            y=df["volume"],
            name="Volume",
            marker_color=bar_colors,
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    # Layout
    fig.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=30, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor="rgba(0,0,0,0)",
        ),
        hovermode="x unified",
        xaxis_rangeslider_visible=False,
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(128,128,128,0.15)",
        zeroline=False,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(128,128,128,0.15)",
        zeroline=False,
    )

    return fig


def render_company_header(record: dict) -> None:
    company = record.get("company_name") or record.get("ticker", "—")
    sector = record.get("sector") or "—"
    industry = record.get("industry") or "—"

    st.markdown(f"## {company}")
    st.markdown(
        f"<span style='color:gray;font-size:0.9rem;'>{sector} &nbsp;·&nbsp; {industry}</span>",
        unsafe_allow_html=True,
    )


def render_metrics(record: dict) -> None:
    visible = [
        (key, label)
        for key, label in _METRIC_LABELS.items()
        if not _is_blank(record.get(key))
    ]

    if not visible:
        st.info("No metrics available for this ticker.")
        return

    cols = st.columns(min(len(visible), 4))
    for idx, (key, label) in enumerate(visible):
        col = cols[idx % len(cols)]
        col.metric(label=label, value=_format_metric(key, record[key]))


def render_chart(ticker: str) -> None:
    st.markdown("---")
    st.subheader("Technical Chart")

    raw = fetch_chart_data(ticker)
    if not raw:
        st.info("No chart data available for this ticker.")
        return

    df = pd.DataFrame(raw)
    df["trading_date"] = pd.to_datetime(df["trading_date"])
    df.sort_values("trading_date", inplace=True)

    required_cols = {"open_price", "high_price", "low_price", "close_price", "volume"}
    missing = required_cols - set(df.columns)
    if missing:
        st.warning(f"Chart data is missing columns: {', '.join(missing)}")
        return

    fig = build_chart(df)
    st.plotly_chart(fig, use_container_width=True)


def render_news(record: dict) -> None:
    title = record.get("news_title")
    if not title:
        return

    st.markdown("---")
    st.subheader("Latest News")

    date_str = record.get("news_date", "")
    score = record.get("sentiment_score")
    summary = record.get("news_summary", "")

    score_label = ""
    if score is not None:
        try:
            s = float(score)
            if s > 0.0:
                label, color = "Bullish", "green"
            elif s < 0.0:
                label, color = "Bearish", "red"
            else:
                label, color = "Neutral", "goldenrod"
            score_label = (
                f" &nbsp; <span style='color:{color};font-weight:600;'>"
                f"{label} ({s:+.3f})</span>"
            )
        except (TypeError, ValueError):
            pass

    header_html = (
        f"<b>{title}</b>"
        + (f" &nbsp; <span style='color:gray;font-size:0.85rem;'>{date_str}</span>" if date_str else "")
        + score_label
    )

    with st.expander(title, expanded=True):
        st.markdown(header_html, unsafe_allow_html=True)
        if summary:
            st.markdown(f"\n{summary}")


def main() -> None:
    tearsheet_data = fetch_tearsheet()

    if not tearsheet_data:
        st.warning("No tearsheet data could be loaded. Check the backend connection.")
        return

    tickers = sorted({row["ticker"] for row in tearsheet_data if row.get("ticker")})

    # Sidebar
    st.sidebar.title("Stock Tearsheet")
    st.sidebar.markdown("---")
    selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Backend: `{API_URL}`")

    # Match ticker to tearsheet row
    record = next(
        (r for r in tearsheet_data if r.get("ticker") == selected_ticker), {}
    )

    if not record:
        st.error(f"No data found for ticker **{selected_ticker}**.")
        return

    # Render sections
    render_company_header(record)
    st.markdown("")  # Spacing
    render_metrics(record)
    render_chart(selected_ticker)
    render_news(record)


if __name__ == "__main__":
    main()
