"""Pre-generate the /onboard sign's spoken stop announcements (AVA) as MP3 clips in media/ava/.

The sign cannot count on the tablet's browser having a working text-to-speech voice, so every line is
rendered ahead of time: one clip per stop NAME (`<slug>.mp3`, shared by every stop id with that name, so a
TransLoc stop renumbering needs nothing regenerated) plus the lead-in phrases (`_this-is.mp3`,
`_the-next-stop-is.mp3`). The page plays lead-in + name back to back.

  pip install piper-tts            (offline neural TTS; needs ffmpeg on PATH too)
  python -m piper.download_voices en_US-ljspeech-high --data-dir <voice dir>
  python scripts/build_ava_clips.py --voice <voice dir>/en_US-ljspeech-high.onnx <routes.json or URL> [...]

Each source is a vehicle-log routes snapshot (`YYYYMMDD_routes.json`, local path or
https://.../vehicle_log/YYYYMMDD_routes.json). Only clips whose text is new or changed are rendered
(media/ava/manifest.json records what each clip says); --force redoes them all, e.g. after changing voice.
If a name is read badly, add it to SAY below and rerun.
"""
import argparse
import json
import re
import subprocess
import sys
import tempfile
import urllib.request
import wave
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parents[1] / "media" / "ava"

# Lead-ins run straight into a stop name ("This is" + "Madison Avenue at Grady Avenue.").
PHRASES = {"_this-is": "This is", "_the-next-stop-is": "The next stop is"}
# Said after "This is X." when the schedule holds the bus there; one clip per whole minute the page announces
# (HOLD_ANNOUNCE_MAX_MIN in html/onboard.html).
PHRASES.update({
    f"_hold-{n}": f"To maintain even spacing of buses, this bus will be held at this stop for approximately {n} minutes."
    for n in range(2, 21)
})

# Whole-name overrides for anything the rules below read badly: stop name -> what to say.
SAY: dict = {}

# "(Northbound)" etc. is dropped: riders already on the bus don't need the direction.
EXPAND = [
    (r"\s*\(EIG\)", ""), (r"\s*\((north|south|east|west)bound\)", ""), (r"\s*\(([^)]*)\)", r", \1"),
    (r"\s*@\s*", " at "), (r"\s*/\s*", " "), ("½", " and a half"),
    (r"\bSt\b\.?", "Street"), (r"\bAve\b\.?", "Avenue"), (r"\bRd\b\.?", "Road"), (r"\bDr\b\.?", "Drive"),
    (r"\bBlvd\b\.?", "Boulevard"), (r"\bCt\b\.?", "Court"), (r"\bApts\b\.?", "Apartments"),
    (r"\bBldg\b\.?", "Building"), (r"\bRec\b", "Recreation"), (r"\bJPJ\b", "J P J"), (r"\bUVA\b", "U V A"),
    (r"\bMR-(\d)", r"M R \1"), (r"\bAFC\b", "A F C"), (r"\bLn\b\.?", "Lane"), (r"\bPl\b\.?", "Place"),
]


def slug(name: str) -> str:
    """File name for a stop name. Must match clipSlug() in html/onboard.html."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def spoken(name: str) -> str:
    if name in SAY:
        return SAY[name]
    text = name
    for pattern, to in EXPAND:
        text = re.sub(pattern, to, text, flags=re.IGNORECASE if "bound" in pattern else 0)
    return re.sub(r"\s+", " ", text).strip() + "."


def stop_names(source: str) -> set:
    if source.startswith("http"):
        with urllib.request.urlopen(source, timeout=30) as r:
            data = json.load(r)
    else:
        data = json.loads(Path(source).read_text(encoding="utf-8"))
    stops = data.get("stops") if isinstance(data, dict) else []
    return {str(s.get("Name") or s.get("Description")).strip() for s in stops or [] if s.get("Name") or s.get("Description")}


def render(voice, text: str, out_path: Path, tail_pause_s: float) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        wav_path = Path(tmp) / "clip.wav"
        with wave.open(str(wav_path), "wb") as wav:
            voice.synthesize_wav(text, wav)
        # Trim the silence at both ends so lead-in + name splice tightly, then add back a fixed pause.
        trim = "silenceremove=start_periods=1:start_threshold=-50dB"
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-i", str(wav_path),
             "-af", f"{trim},areverse,{trim},areverse,apad=pad_dur={tail_pause_s}",
             "-ac", "1", "-ar", "22050", "-b:a", "48k", str(out_path)],
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sources", nargs="+", help="routes snapshot JSON files or URLs")
    parser.add_argument("--voice", required=True, help="path to a Piper voice .onnx")
    parser.add_argument("--force", action="store_true", help="re-render every clip")
    args = parser.parse_args()

    from piper import PiperVoice  # imported late so --help works without it

    names: set = set()
    for source in args.sources:
        names |= stop_names(source)
    wanted = dict(PHRASES)
    for name in sorted(names):
        key = slug(name)
        if key in wanted and wanted[key] != spoken(name):
            sys.exit(f"two stop names share the file name {key!r}: {name!r}")
        wanted[key] = spoken(name)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = OUT_DIR / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    voice_name = Path(args.voice).stem
    redo_all = args.force or manifest.get("voice") != voice_name
    clips = dict(manifest.get("clips") or {})  # clips for stops not in today's sources are kept
    for key in [k for k in clips if k.startswith("_") and k not in PHRASES]:
        del clips[key]  # a lead-in the page no longer uses
        (OUT_DIR / f"{key}.mp3").unlink(missing_ok=True)

    voice = PiperVoice.load(args.voice)
    made = 0
    for key, text in wanted.items():
        out_path = OUT_DIR / f"{key}.mp3"
        if not redo_all and clips.get(key) == text and out_path.exists():
            continue
        render(voice, text, out_path, tail_pause_s=0.35 if text.endswith(".") else 0.05)
        clips[key] = text
        made += 1
        print(f"  {key}.mp3  <- {text}")
    manifest_path.write_text(
        json.dumps({"voice": voice_name, "clips": dict(sorted(clips.items()))}, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"{made} clip(s) rendered, {len(clips)} in {OUT_DIR}")


if __name__ == "__main__":
    main()
