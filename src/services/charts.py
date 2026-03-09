from pathlib import Path
from typing import Dict, List

from .images import generate_image


def render_chart(spec: Dict, output_path: Path) -> None:
    chart_type = (spec.get("type") or "bar").lower()
    title = spec.get("title") or "Chart"
    x_label = spec.get("x_label") or ""
    y_label = spec.get("y_label") or ""

    if chart_type in ("line", "bar"):
        x_values = spec.get("x") or []
        series = spec.get("series") or []
        series_text = "\n".join(
            f"- {item.get('name') or 'Series'}: {item.get('data') or []}" for item in series
        )
        if not series_text:
            series_text = f"- values: {spec.get('values') or []}"
        prompt = (
            "Create a clean, readable chart image. "
            f"Chart type: {chart_type}. Title: {title}. "
            f"X label: {x_label}. Y label: {y_label}. "
            f"X values: {x_values}. "
            f"Series:\n{series_text}\n"
            "Use a white background, clear axis labels, and legible fonts."
        )
    elif chart_type == "pie":
        labels = spec.get("labels") or []
        values = spec.get("values") or []
        prompt = (
            "Create a clean, readable pie chart image. "
            f"Title: {title}. Labels: {labels}. Values: {values}. "
            "Use a white background, clear labels, and legible fonts."
        )
    else:
        prompt = (
            "Create a clean chart image. "
            f"Title: {title}. Data: {spec}. "
            "Use a white background and legible fonts."
        )

    if not generate_image(prompt, output_path):
        raise RuntimeError("Failed to generate chart image")


def build_chart_markdown(title: str, url: str, caption: str | None = None) -> str:
    lines = [f"![{title}]({url})"]
    if caption:
        lines.append(f"*{caption}*")
    return "\n".join(lines)


def normalize_chart_specs(raw: Dict) -> List[Dict]:
    charts = raw.get("charts") if isinstance(raw, dict) else None
    if isinstance(charts, list):
        return charts
    return []
