from unittest import mock

import slides_mirror
from slides_mirror import SlidesMirror, parse_published_deck


def _slide_js(slide_id: str, svg: str) -> str:
    escaped = svg.replace("<", "\\x3c").replace(">", "\\x3e").replace("=", "\\x3d").replace('"', "\\x22").replace("/", "\\/")
    return (
        f"<script>SK_svgData = '{escaped}'; SK_modelChunkLoadStart = 1; "
        f"SK_viewerApp.setPageData('{slide_id}', SK_svgData, []); SK_svgData = undefined;</script>"
    )


def _doc_entry(slide_id, prev, shown):
    link = f'"{slide_id}","{prev}"' if prev else f'"{slide_id}"'
    return f'["{slide_id}",0,"",[],[[1]],true,1000],[{link}],"",["https://x/a"],[],{1 if shown else 0},{{"p9":"u"}},[]]'


def _page(slide_ids, revision="10.0", skipped=()):
    entries = ",".join(_doc_entry(i, slide_ids[n - 1] if n else None, i not in skipped) for n, i in enumerate(slide_ids))
    head = (
        f"<script>viewerData = {{urlPrefix: 'x', title: 'Deck \\x26 Co', revision:  {revision} , "
        f"docData: [[520192,390144],[{entries}]]}};</script>"
    )
    svg = '<svg viewBox="0 0 10 10"><image xlink:href="https://docs.google.com/slides-images-rt/abc?x=1&amp;y=2"/></svg>'
    return head + "".join(_slide_js(i, svg) for i in slide_ids)


def test_parse_published_deck_keeps_order_skip_flags_and_unescapes():
    deck = parse_published_deck(_page(["p1", "g2_0_0", "p5"], skipped={"g2_0_0"}))
    assert deck["title"] == "Deck & Co"
    assert deck["revision"] == "10.0"
    assert [(s[0], s[2]) for s in deck["slides"]] == [("p1", True), ("g2_0_0", False), ("p5", True)]
    assert deck["slides"][0][1].startswith('<svg viewBox="0 0 10 10">')


class _Resp:
    def __init__(self, text="", content=b"", ctype="text/html"):
        self.text, self.content, self.headers = text, content, {"content-type": ctype}

    def raise_for_status(self):
        pass


def _client(page):
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.get.side_effect = lambda url: _Resp(page) if url == "deck" else _Resp(content=b"PNGDATA", ctype="image/png")
    return client


def _refresh(mirror, page):
    with mock.patch.object(slides_mirror.httpx, "Client", return_value=_client(page)):
        return mirror.refresh("deck")


def test_refresh_saves_shown_slides_and_rejects_a_shrunken_deck(tmp_path):
    mirror = SlidesMirror(tmp_path)
    assert _refresh(mirror, _page(["p1", "p2", "p3", "p4"], skipped={"p3"})) is True
    status = mirror.status()
    assert len(status["slides"]) == 3 and status["last_error"] is None
    name = status["slides"][0].rsplit("/", 1)[1]
    svg = mirror.path_for("slide", name).read_text(encoding="utf-8")
    assert "docs.google.com" not in svg and 'xlink:href="/v1/ob-slides/img/' in svg

    assert _refresh(mirror, _page(["p1"], revision="11.0")) is False
    assert "keeping the old copy" in mirror.last_error
    assert len(SlidesMirror(tmp_path).status()["slides"]) == 3

    # Skipping most of the deck on purpose is fine: the shrink check counts the whole deck, skipped slides included.
    _refresh(mirror, _page(["p1", "p2", "p3", "p4"], revision="12.0", skipped={"p2", "p3", "p4"}))
    assert mirror.last_error is None and len(mirror.status()["slides"]) == 1


def test_refresh_rejects_a_page_without_skip_flags(tmp_path):
    mirror = SlidesMirror(tmp_path)
    assert _refresh(mirror, _page(["p1", "p2"]).replace("docData: ", "somethingElse: ")) is False
    assert "skip flag" in mirror.last_error and mirror.status()["slides"] == []


def test_path_for_rejects_anything_but_hashed_names(tmp_path):
    mirror = SlidesMirror(tmp_path)
    assert mirror.path_for("slide", "../manifest.json") is None
    assert mirror.path_for("img", "a" * 24 + ".exe") is None
