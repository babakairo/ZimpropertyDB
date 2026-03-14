"""
analytics/chart_generator.py
Generates PNG trend charts for the property intelligence PDF reports.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def generate_price_trend_chart(
    trend_data: list,
    suburb: str,
    output_path: str,
) -> str:
    """
    Generates a price trend line chart and saves it as a PNG.

    Parameters
    ----------
    trend_data : list
        Output of get_price_trend() — list of dicts with keys
        "week" (str), "median_price" (float), "listing_count" (int).
    suburb : str
        Suburb name, used in the chart title.
    output_path : str
        Full path where the PNG will be saved.

    Returns
    -------
    str
        output_path on success.

    Raises
    ------
    ValueError
        If trend_data is completely empty (None or []).
    """
    if trend_data is None:
        raise ValueError("trend_data must not be None")

    import matplotlib
    matplotlib.use("Agg")   # non-interactive backend — no display needed
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    NAVY  = "#0D1B3E"
    TEAL  = "#00BCD4"
    GREY  = "#AAAAAA"

    fig, ax = plt.subplots(figsize=(7, 3), dpi=150)

    # ── Insufficient data placeholder ────────────────────────────────────────
    if len(trend_data) < 4:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.text(
            0.5, 0.5,
            "Insufficient data for trend chart",
            ha="center", va="center",
            fontsize=11, color=GREY,
            transform=ax.transAxes,
        )
        ax.axis("off")
        fig.patch.set_facecolor("white")
        fig.tight_layout()
        fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return output_path

    # ── Build series ─────────────────────────────────────────────────────────
    weeks  = [d["week"] for d in trend_data]
    prices = [d["median_price"] for d in trend_data]

    ax.plot(weeks, prices, color=TEAL, linewidth=2, marker="o", markersize=4)

    # ── X-axis: show every 3rd label ─────────────────────────────────────────
    tick_positions = list(range(0, len(weeks), 3))
    if (len(weeks) - 1) not in tick_positions:
        tick_positions.append(len(weeks) - 1)
    ax.set_xticks([weeks[i] for i in tick_positions])
    ax.set_xticklabels(
        [weeks[i] for i in tick_positions],
        fontsize=7, color=GREY, rotation=30, ha="right",
    )

    # ── Y-axis: $XXX,XXX format ───────────────────────────────────────────────
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"${x:,.0f}")
    )
    ax.tick_params(axis="y", labelsize=7, labelcolor=GREY)

    # ── Grid: horizontal lines only ──────────────────────────────────────────
    ax.yaxis.grid(True, color="#EEEEEE", linewidth=0.8)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)

    # Remove top / right spines
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#DDDDDD")
    ax.spines["bottom"].set_color("#DDDDDD")

    # ── Title ────────────────────────────────────────────────────────────────
    ax.set_title(
        f"Median Asking Price — {suburb} (Last 12 Weeks)",
        fontsize=11, color=NAVY, pad=8, loc="left",
    )

    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from analytics.suburb_queries import get_price_trend

    suburb = "Borrowdale"
    trend  = get_price_trend(suburb, weeks=12)
    print(f"Trend data: {len(trend)} weeks")

    out = str(Path(__file__).parent.parent / "reports" / "output" / "borrowdale_trend.png")
    generate_price_trend_chart(trend, suburb, out)
    print(f"Chart saved: {out}")

    size = Path(out).stat().st_size
    print(f"File size: {size:,} bytes ({size // 1024} KB)")
