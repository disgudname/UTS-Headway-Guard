"""Local copy of a published Google Slides deck, for the /ob-slides wall screen.

A published deck (docs.google.com/presentation/d/e/<id>/pub) has no export option, but the page Google serves already holds
every slide as a self-contained SVG (text is drawn as paths) in `SK_svgData = '...'; SK_viewerApp.setPageData('<slide id>', ...)`
script blocks, in presentation order. Only the pictures are external (`<image xlink:href="https://docs.google.com/
slides-images-rt/...">`), and those download without a login, animated GIFs included. So a refresh pulls the page, cuts out
the SVGs, saves the pictures, and points each SVG at the saved copies.

Slides marked "Skip slide" in the deck are still in the page. Each slide's entry in `viewerData`'s docData ends in
`["<id>", "<previous id>"], "<speaker notes>", [...image urls], [], <shown>, {...}`, and <shown> is 0 for a skipped slide:
checked against Google's own player on 2026-09-28 (it visited exactly the 11 slides marked 1 out of 23). Skipped slides are
left out.

Speaker notes (HTML, JS-escaped) are in that same entry. A note with a time in it ("5s", "8 sec", "20 seconds") sets how long
that slide stays up; slides without one use the screen's default.

This relies on Google's internal page format, not a public API. A pull that finds no slides, or far fewer than the last good
one, is rejected and the last good copy keeps being served. So is one where a slide's skip flag can't be found, so a
format change can't put skipped slides on the screen.

Files (in <data dir>/slides_mirror/): manifest.json, slides/<sha>.svg, img/<sha>.<ext>. Names are content hashes, so they can
be cached forever by browsers.
"""
from __future__ import annotations

import hashlib
import html
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

MIN_KEEP_FRACTION = 0.5
_SLIDE_RE = re.compile(
    r"SK_svgData = '((?:[^'\\]|\\.)*)';.*?SK_viewerApp\.setPageData\('([^']+)', SK_svgData", re.S
)
# Groups: slide id, speaker notes (still JS-escaped HTML), shown flag.
_SHOWN_RE = re.compile(r'\["([^"]+)"(?:,"[^"]*")?\],"((?:[^"\\]|\\.)*)",\[[^\]]*\],\[\],([01]),\{')
_NOTE_SECONDS_RE = re.compile(r"(?<![\w.])(\d{1,3}(?:\.\d+)?)\s*(?:s|secs?|seconds?)\b", re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_TITLE_RE = re.compile(r"viewerData = \{.*?title: '((?:[^'\\]|\\.)*)'", re.S)
_REVISION_RE = re.compile(r"viewerData = \{.*?revision:\s*([0-9.]+)", re.S)
_IMAGE_HREF_RE = re.compile(r'(<image\b[^>]*?\bxlink:href=")(https?://[^"]+)(")')
_JS_ESCAPE_RE = re.compile(r"\\x([0-9a-fA-F]{2})|\\u([0-9a-fA-F]{4})|\\(.)", re.S)
_IMAGE_EXTS = {"image/png": "png", "image/jpeg": "jpg", "image/gif": "gif", "image/webp": "webp", "image/svg+xml": "svg"}
NAME_RE = re.compile(r"[0-9a-f]{24}\.(svg|png|jpg|gif|webp)")


def _js_unescape(text: str) -> str:
    def rep(m: re.Match) -> str:
        if m.group(1):
            return chr(int(m.group(1), 16))
        if m.group(2):
            return chr(int(m.group(2), 16))
        return {"n": "\n", "t": "\t", "r": "\r"}.get(m.group(3), m.group(3))

    return _JS_ESCAPE_RE.sub(rep, text)


def note_text(raw: str) -> str:
    """Plain text of a slide's speaker notes as they appear in docData (JS-escaped HTML)."""
    return html.unescape(_TAG_RE.sub(" ", _js_unescape(raw))).strip()


def note_seconds(note: str) -> Optional[float]:
    """Display time from a speaker note like "5s" or "20 seconds" (1-600 s), else None."""
    match = _NOTE_SECONDS_RE.search(note)
    return min(600.0, max(1.0, float(match.group(1)))) if match else None


def parse_published_deck(page: str) -> Dict[str, Any]:
    """{title, revision, slides: [(slide_id, svg, shown, notes)]} from a published deck's /pub page, in presentation order.
    shown is None when the slide's skip flag wasn't found."""
    title = _TITLE_RE.search(page)
    revision = _REVISION_RE.search(page)
    doc_data = page[page.find("docData: "):] if "docData: " in page else ""
    meta = {slide_id: (flag == "1", note_text(notes)) for slide_id, notes, flag in _SHOWN_RE.findall(doc_data)}
    return {
        "title": _js_unescape(title.group(1)) if title else "",
        "revision": revision.group(1) if revision else "",
        "slides": [
            (slide_id, _js_unescape(data), *meta.get(slide_id, (None, "")))
            for data, slide_id in _SLIDE_RE.findall(page)
        ],
    }


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:24]


def _write_atomic(path: Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


class SlidesMirror:
    def __init__(self, data_dir: Path, url_prefix: str = "/v1/ob-slides"):
        self.root = Path(data_dir) / "slides_mirror"
        self.slides_dir = self.root / "slides"
        self.img_dir = self.root / "img"
        self.url_prefix = url_prefix
        self.last_error: Optional[str] = None
        self.last_checked: Optional[str] = None
        self.manifest: Dict[str, Any] = {}
        try:
            self.manifest = json.loads((self.root / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass

    def status(self) -> Dict[str, Any]:
        slides = self.manifest.get("slides") or []
        return {
            "title": self.manifest.get("title", ""),
            "revision": self.manifest.get("revision", ""),
            "fetched_at": self.manifest.get("fetched_at"),
            "checked_at": self.last_checked,
            "last_error": self.last_error,
            "slides": [f"{self.url_prefix}/slide/{s['file']}" for s in slides],
            # Per-slide display time from speaker notes (None = the screen's default), same order as "slides".
            "seconds": [s.get("seconds") for s in slides],
        }

    def path_for(self, kind: str, name: str) -> Optional[Path]:
        if not NAME_RE.fullmatch(name):
            return None
        path = (self.slides_dir if kind == "slide" else self.img_dir) / name
        return path if path.is_file() else None

    def refresh(self, deck_url: str) -> bool:
        """Pull the deck and swap in the new copy. Returns True if the served slides changed. Blocking (run in a thread)."""
        self.last_checked = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            with httpx.Client(follow_redirects=True, timeout=60) as client:
                resp = client.get(deck_url)
                resp.raise_for_status()
                deck = parse_published_deck(resp.text)
                slides = deck["slides"]
                # Compared on the whole deck, skipped slides included, so skipping lots of slides on purpose isn't rejected.
                previous = self.manifest.get("deck_slides") or 0
                if not slides:
                    raise ValueError("no slides found in the published page (Google may have changed its format)")
                if previous and len(slides) < previous * MIN_KEEP_FRACTION:
                    raise ValueError(f"only {len(slides)} slides found (last good copy had {previous}); keeping the old copy")
                unknown = [slide_id for slide_id, _, shown, _ in slides if shown is None]
                if unknown:
                    raise ValueError(f"skip flag not found for {len(unknown)} slide(s) (Google may have changed its format)")
                if deck["revision"] and deck["revision"] == self.manifest.get("revision") and len(slides) == previous:
                    self.last_error = None
                    return False
                self.slides_dir.mkdir(parents=True, exist_ok=True)
                self.img_dir.mkdir(parents=True, exist_ok=True)
                images: Dict[str, str] = {}
                entries: List[Dict[str, Any]] = []
                for slide_id, svg, shown, notes in slides:
                    if not shown:
                        continue
                    svg = _IMAGE_HREF_RE.sub(lambda m: m.group(1) + self._save_image(client, m.group(2), images) + m.group(3), svg)
                    data = svg.encode("utf-8")
                    name = _sha(data) + ".svg"
                    if not (self.slides_dir / name).exists():
                        _write_atomic(self.slides_dir / name, data)
                    entries.append({"id": slide_id, "file": name, "seconds": note_seconds(notes)})
        except Exception as exc:
            self.last_error = str(exc)
            return False
        old = self.manifest
        def served(slides):
            return [(e["file"], e.get("seconds")) for e in slides or []]

        changed = served(entries) != served(old.get("slides"))
        self.manifest = {
            "title": deck["title"],
            "revision": deck["revision"],
            "fetched_at": self.last_checked,
            "deck_slides": len(slides),
            "slides": entries,
            "images": sorted(set(images.values())),
        }
        _write_atomic(self.root / "manifest.json", json.dumps(self.manifest, indent=1).encode("utf-8"))
        self.last_error = None
        # Keep the previous copy's files too, so a screen partway through a loop doesn't hit a missing slide or picture.
        self._prune(
            {e["file"] for m in (old, self.manifest) for e in m.get("slides") or []},
            {name for m in (old, self.manifest) for name in m.get("images") or []},
        )
        return changed

    def _save_image(self, client: httpx.Client, href: str, images: Dict[str, str]) -> str:
        url = html.unescape(href)
        if url not in images:
            resp = client.get(url)
            resp.raise_for_status()
            ctype = resp.headers.get("content-type", "").split(";")[0].strip().lower()
            ext = _IMAGE_EXTS.get(ctype)
            if not ext:
                raise ValueError(f"unexpected image type {ctype or '(none)'} in slide")
            name = f"{_sha(resp.content)}.{ext}"
            if not (self.img_dir / name).exists():
                _write_atomic(self.img_dir / name, resp.content)
            images[url] = name
        return f"{self.url_prefix}/img/{images[url]}"

    def _prune(self, keep_slides: set, keep_images: set) -> None:
        for folder, keep in ((self.slides_dir, keep_slides), (self.img_dir, keep_images)):
            for path in folder.iterdir():
                if path.name not in keep:
                    try:
                        path.unlink()
                    except OSError:
                        pass
