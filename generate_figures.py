"""
generate_figures.py
Regenerates all publication figures for main.tex.
Run from the project root: python generate_figures.py
Outputs PNG files to the project root and an HTML preview to figures_preview.html
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
HTML_SECTIONS = []

# ── shared style ────────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.dpi": 150,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

BLUE   = "#2563EB"
GREEN  = "#16A34A"
ORANGE = "#EA580C"
GRAY   = "#6B7280"
RED    = "#DC2626"

def save(fig, name, title_html):
    path = os.path.join(ROOT, name)
    fig.savefig(path, bbox_inches="tight", dpi=150)
    plt.close(fig)
    # embed as base64 for HTML preview
    import base64
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    HTML_SECTIONS.append(
        f'<h2>{title_html}</h2>'
        f'<p style="font-family:monospace;color:#555">{name}</p>'
        f'<img src="data:image/png;base64,{b64}" '
        f'style="max-width:800px;border:1px solid #ccc;padding:4px">'
    )
    print(f"[OK] {name}")


# ── Figure 2: LLM Test Quality by Model ─────────────────────────────────────
def fig_llm_quality():
    models     = ["dim_teams", "fct_matches", "fct_training_dataset"]
    useful     = [1, 5, 3]
    redundant  = [2, 2, 0]
    low_value  = [0, 6, 6]

    x   = np.arange(len(models))
    w   = 0.24
    fig, ax = plt.subplots(figsize=(7.5, 4.2))

    b1 = ax.bar(x - w,   useful,    w, label="Useful",              color=GREEN,  edgecolor="white")
    b2 = ax.bar(x,        redundant, w, label="Redundant",           color=ORANGE, edgecolor="white")
    b3 = ax.bar(x + w,   low_value, w, label="Executable Low-Value", color=GRAY,   edgecolor="white")

    for bars in (b1, b2, b3):
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 0.08,
                        str(int(h)), ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels([m.replace("_", "\n") for m in models], fontsize=10)
    ax.set_ylabel("Number of Generated Tests")
    ax.set_ylim(0, 8.5)
    ax.set_title("LLM-Generated dbt Test Quality by Analytical Model\n"
                 "(Total: 25 generated — 9 useful, 4 redundant, 12 low-value, 0 invalid)",
                 pad=10)
    ax.legend(loc="upper right")

    # totals annotation
    totals = [u + r + l for u, r, l in zip(useful, redundant, low_value)]
    for xi, tot in zip(x, totals):
        ax.text(xi, tot + 0.5, f"n={tot}", ha="center", va="bottom",
                fontsize=9, color="#374151", fontweight="bold")

    fig.tight_layout()
    save(fig, "fig2_llm_test_quality_updated.png",
         "Figure 2 — LLM-Generated dbt Test Quality by Analytical Model")


# ── Figure: Detection Coverage ───────────────────────────────────────────────
def fig_detection_coverage():
    batches       = ["Batch A\nKey Integrity\n(n=4)",
                     "Batch B\nSemantic/Domain\n(n=6)",
                     "Batch C\nMixed Full-Stack\n(n=6)"]
    manual_only   = [4, 0, 3]
    man_expanded  = [4, 6, 6]
    man_llm       = [4, 6, 6]
    totals        = [4, 6, 6]

    x = np.arange(len(batches))
    w = 0.26
    fig, ax = plt.subplots(figsize=(8.5, 4.8))

    b1 = ax.bar(x - w, manual_only,  w, label="Manual-Only (weak baseline)", color=RED,   edgecolor="white", alpha=0.85)
    b2 = ax.bar(x,     man_expanded, w, label="Manual-Expanded (strong comparator)", color=BLUE,  edgecolor="white", alpha=0.85)
    b3 = ax.bar(x + w, man_llm,      w, label="Manual + LLM",                 color=GREEN, edgecolor="white", alpha=0.85)

    for bars, vals, tot_list in [(b1, manual_only, totals),
                                  (b2, man_expanded, totals),
                                  (b3, man_llm, totals)]:
        for bar, v, t in zip(bars, vals, tot_list):
            ax.text(bar.get_x() + bar.get_width() / 2, v + 0.07,
                    f"{v}/{t}", ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(batches, fontsize=9.5)
    ax.set_ylabel("Anomalies Detected")
    ax.set_ylim(0, 8.2)
    ax.set_title("Anomaly-Detection Coverage Across Three Experimental Batches\n"
                 "Manual-Only: 7/16 total  |  Manual-Expanded: 16/16  |  Manual + LLM: 16/16",
                 pad=10)
    ax.legend(loc="upper left", framealpha=0.9)

    # overall summary bar at bottom
    ax.axhline(y=0, color="black", linewidth=0.8)
    fig.tight_layout()
    save(fig, "fig_detection_coverage_updated.png",
         "Figure — Anomaly Detection Coverage by Batch and Condition")


# ── Figure: Pipeline Runtime Breakdown ───────────────────────────────────────
def fig_runtime():
    stages = [
        "Compile Research\nSummary",
        "Usefulness Audit",
        "dbt Run\n(DuckDB Full Refresh)",
        "dbt Test\n(Merged LLM Layer)",
        "Generate LLM\ndbt Tests",
        "Validate DuckDB\nvs Snowflake",
        "Migrate DuckDB\nto Snowflake",
        "Multi-Batch Anomaly\nExperiment",
    ]
    runtimes = [0.018, 0.152, 6.997, 7.835, 10.707, 14.130, 22.643, 44.095]

    colors_list = []
    for r in runtimes:
        if r < 1:
            colors_list.append(GRAY)
        elif r < 15:
            colors_list.append(BLUE)
        else:
            colors_list.append(ORANGE)

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(stages, runtimes, color=colors_list, edgecolor="white", height=0.6)

    for bar, v in zip(bars, runtimes):
        ax.text(v + 0.3, bar.get_y() + bar.get_height() / 2,
                f"{v:.3f}s", va="center", fontsize=9.5)

    ax.set_xlabel("Duration (seconds)")
    ax.set_xlim(0, 52)
    ax.set_title(f"Instrumented Workflow Runtime Breakdown\nTotal: 106.577 s across 8 logged stages",
                 pad=10)

    legend_patches = [
        mpatches.Patch(color=ORANGE, label="Evaluation-heavy stages (>15 s)"),
        mpatches.Patch(color=BLUE,   label="Core workflow stages (1–15 s)"),
        mpatches.Patch(color=GRAY,   label="Lightweight stages (<1 s)"),
    ]
    ax.legend(handles=legend_patches, loc="lower right", framealpha=0.9)
    ax.axvline(x=0, color="black", linewidth=0.8)

    fig.tight_layout()
    save(fig, "fig3_pipeline_runtime_breakdown_updated.png",
         "Figure 3 — Pipeline Runtime Breakdown")


# ── Figure: Cross-Store Validation ───────────────────────────────────────────
def fig_cross_store():
    tables     = ["dim_teams", "fct_matches", "fct_training_dataset"]
    duck_rows  = [20, 100, 100]
    snow_rows  = [20, 100, 100]

    x = np.arange(len(tables))
    w = 0.35
    fig, ax = plt.subplots(figsize=(7.5, 4.2))

    b1 = ax.bar(x - w/2, duck_rows, w, label="DuckDB (local)",   color=BLUE,  edgecolor="white", alpha=0.9)
    b2 = ax.bar(x + w/2, snow_rows, w, label="Snowflake (cloud)", color=GREEN, edgecolor="white", alpha=0.9)

    for bar in list(b1) + list(b2):
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 1,
                str(int(h)), ha="center", va="bottom", fontsize=10)

    # MATCH badges
    for xi in x:
        ax.text(xi, max(duck_rows[xi - len(x) if xi == len(x) else xi],
                        snow_rows[xi - len(x) if xi == len(x) else xi]) + 8,
                "[MATCH]", ha="center", va="bottom", fontsize=9,
                color=GREEN, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([t.replace("_", "\n") for t in tables], fontsize=10)
    ax.set_ylabel("Row Count")
    ax.set_ylim(0, 130)
    ax.set_title("Post-Migration Cross-Store Validation: DuckDB vs. Snowflake\n"
                 "Row counts, checksums, and normalized row comparisons all matched exactly",
                 pad=10)
    ax.legend(loc="upper right")
    fig.tight_layout()
    save(fig, "fig_cross_store_validation_updated.png",
         "Figure — Post-Migration Cross-Store Validation (DuckDB vs. Snowflake)")


# ── Run all ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating figures...")
    fig_llm_quality()
    fig_detection_coverage()
    fig_runtime()
    fig_cross_store()

    # Write HTML preview
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Figure Preview — Football Analytics Platform</title>
<style>
  body { font-family: Georgia, serif; max-width: 900px; margin: 40px auto; color: #1f2937; }
  h1   { border-bottom: 2px solid #2563EB; padding-bottom: 8px; }
  h2   { color: #2563EB; margin-top: 40px; }
  img  { display: block; margin: 12px 0; }
</style>
</head>
<body>
<h1>Publication Figure Preview</h1>
<p>All figures use the <strong>main workflow</strong> numbers (25&nbsp;/&nbsp;9&nbsp;/&nbsp;4&nbsp;/&nbsp;12&nbsp;/&nbsp;0 for usefulness; 7/16, 16/16, 16/16 for detection).</p>
""" + "\n".join(HTML_SECTIONS) + "\n</body></html>"

    html_path = os.path.join(ROOT, "figures_preview.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] figures_preview.html")
    print("\nAll done.")
