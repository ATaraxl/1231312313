import argparse
import difflib
import hashlib
import json
import logging
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Pattern, Set, Tuple

# ===================== ⚙️ 全局配置 =====================

UNCATEGORIZED_NAME = "未分类归档"
SUSPECT_REVIEW_NAME = "疑似重复待确认"
DUPLICATE_REVIEW_NAME = "重复区"
HISTORY_DIR_NAME = ".history"
LOG_FILE_NAME = "整理日志.txt"
DEFAULT_SOURCE_DIR = Path(r"F:\mh")


class ScanMode(str, Enum):
    SAFE = "safe"      # 根目录 + 未分类归档（仅直接子文件）
    REPAIR = "repair"  # 再加一级子目录（仅直接子文件）
    FULL = "full"      # 递归全扫描


class MatchType(str, Enum):
    CIRCLE = "CIRCLE"
    COMMERCIAL = "COMMERCIAL"
    LITERAL = "LITERAL"
    UNCATEGORIZED = "UNCATEGORIZED"


class FileStatus(str, Enum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"
    DUPLICATE = "duplicate"
    SUSPECT = "suspect"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class DetectionResult:
    folder_name: str
    match_type: MatchType


@dataclass
class MovePlan:
    source: Path
    target_folder: Path
    destination: Path
    detection: DetectionResult
    status: FileStatus = FileStatus.PENDING
    error: Optional[str] = None
    folder_created: bool = False
    duplicate_of: Optional[str] = None
    suspect_of: Optional[str] = None
    moved_to_duplicate_review: bool = False


@dataclass
class ExecutionSession:
    session_id: str
    scan_mode: ScanMode
    dry_run: bool
    created_at: str
    session_dir: Path
    plan_file: Path
    state_file: Path
    log_file: Path


@dataclass
class OrganizerConfig:
    """配置类：集中管理所有参数"""

    source_dir: Path

    allowed_extensions: Set[str] = field(
        default_factory=lambda: {
            ".zip", ".rar", ".7z", ".tar", ".gz", ".lz",
            ".cbz", ".cbr", ".cb7", ".cbt",
            ".epub", ".pdf", ".mobi", ".azw3",
        }
    )
    fuzzy_threshold: float = 0.85
    duplicate_check: bool = True
    quick_hash_bytes: int = 4 * 1024 * 1024
    suspect_check: bool = True
    suspect_review_dir_name: str = SUSPECT_REVIEW_NAME
    duplicate_review_dir_name: str = DUPLICATE_REVIEW_NAME
    suspect_ignored_tags: Set[str] = field(
        default_factory=lambda: {
            "DL版", "DL", "Digital",
            "中国翻译", "中国翻訳", "汉化", "漢化", "翻译", "翻訳", "机翻", "機翻",
            "修正", "修正版", "无修正", "無修正",
            "English", "Eng", "Japanese", "Sample", "Mosaic", "Decensored",
        }
    )

    bad_suffix_regex: Pattern = re.compile(
        r"(汉化|漢化|翻译|翻訳|机翻|機翻|润色|潤色|改图|嵌字|组|組|版|社|制作|出品|合成|Collection|Works|Art|CG|AI|Created|Sample|Mosaic|Decensored)$",
        re.IGNORECASE,
    )

    bad_full_match: Set[str] = field(
        default_factory=lambda: {
            "Chinese", "Eng", "English", "Japanese", "DL", "Digital", "Comic",
            "Komga", "Color", "Full Color", "Total", "修正", "无修正", "無修正",
            "去码", "不咕鸟", "脸肿", "黑条", "白条",
        }
    )

    generic_names: Set[str] = field(
        default_factory=lambda: {
            "chapter", "vol", "volume", "no", "ep", "episode",
            "image", "picture", "photo", "scan", "dl", "download",
            "COMIC", "Manga", "Doujin", "Anthology", "Pixiv", "Twitter",
            "01", "02", "03", "1", "2", "3",
        }
    )

    commercial_pattern: Pattern = re.compile(
        r"^(?P<title>.+?)(\s*[\(\[（]?)(\s*(v|vol|ch|ep|no|第)[\.]?\s*\d+)",
        re.IGNORECASE,
    )
    jp_kana: Pattern = re.compile(r"[\u3040-\u309F\u30A0-\u30FF]")
    cn_chars: Pattern = re.compile(r"[\u4E00-\u9FFF]")

    bad_full_match_lower: Set[str] = field(init=False, repr=False)
    generic_names_lower: Set[str] = field(init=False, repr=False)
    suspect_ignored_tags_lower: Set[str] = field(init=False, repr=False)

    def __post_init__(self):
        self.source_dir = self.source_dir.expanduser()
        self.allowed_extensions = {ext.lower() for ext in self.allowed_extensions}
        self.bad_full_match_lower = {x.casefold() for x in self.bad_full_match}
        self.generic_names_lower = {x.casefold() for x in self.generic_names}
        self.suspect_ignored_tags_lower = {
            re.sub(r"\s+", "", x).casefold() for x in self.suspect_ignored_tags
        }
        self.quick_hash_bytes = max(1024, int(self.quick_hash_bytes))


class FolderIndex:
    def __init__(self, source_dir: Path, fuzzy_threshold: float):
        self.source_dir = source_dir
        self.fuzzy_threshold = fuzzy_threshold
        self.existing_folders: List[str] = []
        self._known_names: Set[str] = set()

    def refresh(self):
        if not self.source_dir.exists():
            self.existing_folders = []
            self._known_names = set()
            return

        folders = [
            item.name
            for item in self.source_dir.iterdir()
            if item.is_dir() and item.name not in {UNCATEGORIZED_NAME, SUSPECT_REVIEW_NAME, DUPLICATE_REVIEW_NAME}
        ]
        self.existing_folders = sorted(folders, key=str.casefold)
        self._known_names = set(self.existing_folders)

    def match(self, name: str) -> Optional[str]:
        if not self.existing_folders:
            return None
        matches = difflib.get_close_matches(
            name,
            self.existing_folders,
            n=1,
            cutoff=self.fuzzy_threshold,
        )
        return matches[0] if matches else None

    def remember(self, name: str):
        if not name or name in {UNCATEGORIZED_NAME, SUSPECT_REVIEW_NAME} or name in self._known_names:
            return
        self._known_names.add(name)
        self.existing_folders.append(name)


class SeriesNameDetector:
    def __init__(self, config: OrganizerConfig, folder_index: FolderIndex):
        self.cfg = config
        self.folder_index = folder_index

    def detect(self, filename: str) -> DetectionResult:
        stem = Path(filename).stem
        clean_stem = re.sub(r"^\([a-zA-Z]*\d{2,4}\)\s*", "", stem).strip()

        candidate, match_type = self._pick_candidate(clean_stem)
        if not candidate:
            return DetectionResult(UNCATEGORIZED_NAME, MatchType.UNCATEGORIZED)

        final_name = self._finalize_candidate(candidate, match_type)
        if not final_name:
            return DetectionResult(UNCATEGORIZED_NAME, MatchType.UNCATEGORIZED)

        fuzzy_target = self.folder_index.match(final_name)
        resolved_name = fuzzy_target or final_name
        self.folder_index.remember(resolved_name)
        return DetectionResult(resolved_name, match_type)

    def _pick_candidate(self, clean_stem: str) -> Tuple[Optional[str], MatchType]:
        brackets = re.findall(r"\[([^\]]+)\]", clean_stem)
        best_score = -100
        best_candidate = None
        for index, content in enumerate(brackets):
            score = self._calculate_score(content, index)
            if score > best_score and score > -100:
                best_score = score
                best_candidate = content

        if best_candidate:
            return best_candidate, MatchType.CIRCLE

        match = self.cfg.commercial_pattern.search(clean_stem)
        if match:
            return match.group("title").strip(), MatchType.COMMERCIAL

        if self._is_valid_series_name(clean_stem):
            return clean_stem, MatchType.LITERAL

        return None, MatchType.UNCATEGORIZED

    def _finalize_candidate(self, candidate: str, match_type: MatchType) -> Optional[str]:
        final_name = self._normalize_circle_name(candidate)
        final_name = self._sanitize_filename(final_name)

        if match_type == MatchType.CIRCLE and not final_name.startswith("["):
            final_name = f"[{final_name}]"

        if not self._is_valid_series_name(final_name):
            return None
        return final_name

    def _sanitize_filename(self, name: str) -> str:
        return re.sub(r'[\\/:*?"<>|]', '_', name).strip().strip('.')

    def _normalize_circle_name(self, text: str) -> str:
        cleaned = re.sub(r'\s*[\(\（][^\)\）]*[\)\）]', '', text)
        cleaned = cleaned.strip(" -_")
        return cleaned if cleaned else text.strip()

    def _is_valid_series_name(self, name: str) -> bool:
        if not name:
            return False
        clean_name = name.strip()
        normalized_name = clean_name.casefold()
        if len(clean_name) < 2:
            return False
        if clean_name.isdigit():
            return False
        if normalized_name in self.cfg.bad_full_match_lower:
            return False
        if normalized_name in self.cfg.generic_names_lower:
            return False
        return True

    def _calculate_score(self, text: str, index: int) -> int:
        text = text.strip()
        normalized_text = text.casefold()
        if self.cfg.bad_suffix_regex.search(text):
            return -999
        if normalized_text in self.cfg.bad_full_match_lower:
            return -999

        score = 0
        if re.search(r'[\(\（][^\)\）]+[\)\）]', text):
            score += 500
        if self.cfg.jp_kana.search(text):
            score += 300
        elif self.cfg.cn_chars.search(text):
            score += 100
        else:
            score += 50
        if index == 0:
            score += 200
        return score


class SessionManager:
    def __init__(self, script_dir: Path):
        self.script_dir = script_dir
        self.history_dir = self.script_dir / HISTORY_DIR_NAME
        self.history_dir.mkdir(parents=True, exist_ok=True)

    def new_session(self, scan_mode: ScanMode, dry_run: bool) -> ExecutionSession:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        session_id = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{scan_mode.value}"
        if dry_run:
            session_id += "-dryrun"

        session_dir = self.history_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        return ExecutionSession(
            session_id=session_id,
            scan_mode=scan_mode,
            dry_run=dry_run,
            created_at=created_at,
            session_dir=session_dir,
            plan_file=session_dir / "plan.json",
            state_file=session_dir / "state.json",
            log_file=session_dir / "log.txt",
        )

    def save_plan(self, session: ExecutionSession, plans: List[MovePlan]):
        self._write_json(session.plan_file, self._serialize_plans(plans))

    def update_state(self, session: ExecutionSession, plans: List[MovePlan]):
        payload = {
            "session_id": session.session_id,
            "scan_mode": session.scan_mode.value,
            "dry_run": session.dry_run,
            "created_at": session.created_at,
            "plans": self._serialize_plans(plans),
        }
        self._write_json(session.state_file, payload)

    def list_sessions(self) -> List[Dict[str, str]]:
        sessions: List[Dict[str, str]] = []
        if not self.history_dir.exists():
            return sessions

        for session_dir in sorted(self.history_dir.iterdir(), reverse=True):
            if not session_dir.is_dir():
                continue
            try:
                payload = self._read_session_payload(
                    state_file=session_dir / "state.json",
                    plan_file=session_dir / "plan.json",
                    session_dir=session_dir,
                )
                plans = payload.get("plans", [])
                done_count = sum(1 for item in plans if item.get("status") == FileStatus.DONE.value)
                failed_count = sum(1 for item in plans if item.get("status") == FileStatus.FAILED.value)
                skipped_count = sum(1 for item in plans if item.get("status") == FileStatus.SKIPPED.value)
                duplicate_count = sum(1 for item in plans if item.get("status") == FileStatus.DUPLICATE.value)
                suspect_count = sum(1 for item in plans if item.get("status") == FileStatus.SUSPECT.value)
                rolled_back_count = sum(1 for item in plans if item.get("status") == FileStatus.ROLLED_BACK.value)
                duplicate_moved_count = sum(1 for item in plans if item.get("moved_to_duplicate_review"))
                total_count = len(plans)
                summary = (
                    f"total={total_count}, done={done_count}, failed={failed_count}, skipped={skipped_count}, "
                    f"duplicate={duplicate_count}, duplicate_moved={duplicate_moved_count}, "
                    f"suspect={suspect_count}, rollback={rolled_back_count}"
                )
                sessions.append(
                    {
                        "session_id": str(payload.get("session_id", session_dir.name)),
                        "scan_mode": str(payload.get("scan_mode", "unknown")),
                        "dry_run": str(payload.get("dry_run", False)),
                        "created_at": str(payload.get("created_at", "")),
                        "summary": summary,
                    }
                )
            except Exception:
                sessions.append(
                    {
                        "session_id": session_dir.name,
                        "scan_mode": "unknown",
                        "dry_run": "unknown",
                        "created_at": "",
                        "summary": "读取失败",
                    }
                )
        return sessions

    def load_session(self, session_id: str) -> Tuple[ExecutionSession, List[MovePlan]]:
        if session_id == "latest":
            session_id = self._latest_session_id()
            if not session_id:
                raise FileNotFoundError("找不到可回滚的历史会话")

        session_dir = self.history_dir / session_id
        if not session_dir.exists():
            raise FileNotFoundError(f"会话目录不存在: {session_dir}")

        payload = self._read_session_payload(
            state_file=session_dir / "state.json",
            plan_file=session_dir / "plan.json",
            session_dir=session_dir,
        )
        session = ExecutionSession(
            session_id=str(payload["session_id"]),
            scan_mode=ScanMode(str(payload["scan_mode"])),
            dry_run=bool(payload["dry_run"]),
            created_at=str(payload.get("created_at", "")),
            session_dir=session_dir,
            plan_file=session_dir / "plan.json",
            state_file=session_dir / "state.json",
            log_file=session_dir / "log.txt",
        )
        plans = [self._deserialize_plan(item) for item in payload.get("plans", [])]
        return session, plans

    def _latest_session_id(self) -> Optional[str]:
        sessions = self.list_sessions()
        for item in sessions:
            if item["dry_run"] == "False":
                return item["session_id"]
        return None

    def _read_session_payload(self, state_file: Path, plan_file: Path, session_dir: Path) -> Dict[str, object]:
        state_payload = self._try_read_json(state_file)
        if isinstance(state_payload, dict):
            return state_payload

        plan_payload = self._try_read_json(plan_file)
        if not isinstance(plan_payload, list):
            raise ValueError(f"状态文件损坏且无法从计划文件恢复: {session_dir}")

        return {
            "session_id": session_dir.name,
            "scan_mode": self._infer_scan_mode_from_session_id(session_dir.name),
            "dry_run": session_dir.name.endswith("-dryrun"),
            "created_at": "",
            "plans": self._recover_plans_from_plan_data(plan_payload),
        }

    def _try_read_json(self, file_path: Path) -> Optional[object]:
        if not file_path.exists():
            return None

        text = file_path.read_text(encoding="utf-8").strip()
        if not text:
            return None

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    def _recover_plans_from_plan_data(self, plan_payload: List[Dict[str, object]]) -> List[Dict[str, object]]:
        recovered: List[Dict[str, object]] = []
        for item in plan_payload:
            recovered_item = dict(item)
            recovered_item["status"] = self._recover_plan_status(item).value
            recovered.append(recovered_item)
        return recovered

    def _recover_plan_status(self, item: Dict[str, object]) -> FileStatus:
        raw_status = item.get("status")
        if raw_status is not None:
            try:
                stored_status = FileStatus(str(raw_status))
                if stored_status in {
                    FileStatus.SKIPPED,
                    FileStatus.DUPLICATE,
                    FileStatus.SUSPECT,
                    FileStatus.ROLLED_BACK,
                    FileStatus.FAILED,
                }:
                    return stored_status
            except ValueError:
                pass
        return self._infer_status_from_paths(item)

    def _infer_status_from_paths(self, item: Dict[str, object]) -> FileStatus:
        source = Path(str(item["source"]))
        destination = Path(str(item["destination"]))

        if source == destination:
            return FileStatus.SKIPPED
        if destination.exists():
            return FileStatus.DONE
        if source.exists():
            return FileStatus.PENDING
        return FileStatus.FAILED

    def _infer_scan_mode_from_session_id(self, session_id: str) -> str:
        for mode in ScanMode:
            token = f"-{mode.value}"
            if token in session_id:
                return mode.value
        return ScanMode.SAFE.value

    def _serialize_plans(self, plans: List[MovePlan]) -> List[Dict[str, object]]:
        return [
            {
                "source": str(plan.source),
                "target_folder": str(plan.target_folder),
                "destination": str(plan.destination),
                "detection": {
                    "folder_name": plan.detection.folder_name,
                    "match_type": plan.detection.match_type.value,
                },
                "status": plan.status.value,
                "error": plan.error,
                "folder_created": plan.folder_created,
                "duplicate_of": plan.duplicate_of,
                "suspect_of": plan.suspect_of,
                "moved_to_duplicate_review": plan.moved_to_duplicate_review,
            }
            for plan in plans
        ]

    def _deserialize_plan(self, item: Dict[str, object]) -> MovePlan:
        detection_dict = item["detection"]
        detection = DetectionResult(
            folder_name=str(detection_dict["folder_name"]),
            match_type=MatchType(str(detection_dict["match_type"])),
        )
        return MovePlan(
            source=Path(str(item["source"])),
            target_folder=Path(str(item["target_folder"])),
            destination=Path(str(item["destination"])),
            detection=detection,
            status=FileStatus(str(item.get("status", FileStatus.PENDING.value))),
            error=item.get("error") and str(item.get("error")),
            folder_created=bool(item.get("folder_created", False)),
            duplicate_of=item.get("duplicate_of") and str(item.get("duplicate_of")),
            suspect_of=item.get("suspect_of") and str(item.get("suspect_of")),
            moved_to_duplicate_review=bool(item.get("moved_to_duplicate_review", False)),
        )

    def _write_json(self, file_path: Path, payload: object):
        temp_file = file_path.with_name(file_path.name + ".tmp")
        temp_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp_file, file_path)


@dataclass(frozen=True)
class DuplicateCandidate:
    compare_path: Path
    display_path: Path


class DuplicateDetector:
    def __init__(self, config: OrganizerConfig):
        self.cfg = config
        self._scan_paths: Set[Path] = set()
        self._existing_cache: Dict[Path, Dict[Tuple[str, int], List[DuplicateCandidate]]] = {}
        self._planned_cache: Dict[Path, Dict[Tuple[str, int], List[DuplicateCandidate]]] = {}
        self._size_cache: Dict[Path, int] = {}
        self._quick_hash_cache: Dict[Path, str] = {}
        self._full_hash_cache: Dict[Path, str] = {}

    def start_scan(self, scan_paths: Iterable[Path]):
        self._scan_paths = {path.resolve() for path in scan_paths}
        self._existing_cache.clear()
        self._planned_cache.clear()
        self._size_cache.clear()
        self._quick_hash_cache.clear()
        self._full_hash_cache.clear()

    def find_duplicate(self, source: Path, target_folder: Path) -> Optional[DuplicateCandidate]:
        if not self.cfg.duplicate_check or not source.exists() or not source.is_file():
            return None

        folder_key = target_folder.resolve()
        key = self._candidate_key(source)
        existing_candidates = self._load_existing_candidates(folder_key, target_folder).get(key, [])
        planned_candidates = self._planned_cache.get(folder_key, {}).get(key, [])
        source_key = source.resolve()

        for candidate in [*existing_candidates, *planned_candidates]:
            try:
                if candidate.compare_path.resolve() == source_key:
                    continue
                if self._same_file(source, candidate.compare_path):
                    return candidate
            except OSError:
                continue
        return None

    def remember_planned(self, target_folder: Path, source: Path, destination: Path):
        if not self.cfg.duplicate_check:
            return
        folder_key = target_folder.resolve()
        key = self._candidate_key(source)
        bucket = self._planned_cache.setdefault(folder_key, {}).setdefault(key, [])
        bucket.append(DuplicateCandidate(compare_path=source, display_path=destination))

    def _load_existing_candidates(
        self,
        folder_key: Path,
        target_folder: Path,
    ) -> Dict[Tuple[str, int], List[DuplicateCandidate]]:
        if folder_key in self._existing_cache:
            return self._existing_cache[folder_key]

        grouped: Dict[Tuple[str, int], List[DuplicateCandidate]] = {}
        if target_folder.exists() and target_folder.is_dir():
            for path in target_folder.iterdir():
                try:
                    resolved = path.resolve()
                except OSError:
                    continue
                if resolved in self._scan_paths:
                    continue
                if not path.is_file() or path.suffix.lower() not in self.cfg.allowed_extensions:
                    continue
                key = self._candidate_key(path)
                grouped.setdefault(key, []).append(
                    DuplicateCandidate(compare_path=path, display_path=path)
                )

        self._existing_cache[folder_key] = grouped
        return grouped

    def _candidate_key(self, path: Path) -> Tuple[str, int]:
        return (path.suffix.lower(), self._file_size(path))

    def _same_file(self, left: Path, right: Path) -> bool:
        if left.suffix.lower() != right.suffix.lower():
            return False
        if self._file_size(left) != self._file_size(right):
            return False
        if self._quick_hash(left) != self._quick_hash(right):
            return False
        return self._full_hash(left) == self._full_hash(right)

    def _file_size(self, path: Path) -> int:
        cache_key = path.resolve()
        cached = self._size_cache.get(cache_key)
        if cached is not None:
            return cached
        size = path.stat().st_size
        self._size_cache[cache_key] = size
        return size

    def _quick_hash(self, path: Path) -> str:
        cache_key = path.resolve()
        cached = self._quick_hash_cache.get(cache_key)
        if cached is not None:
            return cached

        hasher = hashlib.sha1()
        size = self._file_size(path)
        limit = self.cfg.quick_hash_bytes

        with path.open("rb") as handle:
            if size <= limit * 2:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    hasher.update(chunk)
            else:
                hasher.update(handle.read(limit))
                handle.seek(size - limit)
                hasher.update(handle.read(limit))

        digest = hasher.hexdigest()
        self._quick_hash_cache[cache_key] = digest
        return digest

    def _full_hash(self, path: Path) -> str:
        cache_key = path.resolve()
        cached = self._full_hash_cache.get(cache_key)
        if cached is not None:
            return cached

        hasher = hashlib.sha1()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)

        digest = hasher.hexdigest()
        self._full_hash_cache[cache_key] = digest
        return digest


@dataclass(frozen=True)
class SuspectCandidate:
    compare_path: Path
    display_path: Path
    title_key: str


class SuspectDetector:
    def __init__(self, config: OrganizerConfig):
        self.cfg = config
        self._scan_paths: Set[Path] = set()
        self._existing_cache: Dict[Path, List[SuspectCandidate]] = {}
        self._planned_cache: Dict[Path, List[SuspectCandidate]] = {}
        self._tag_tail_pattern = re.compile(r"\s*[\[\(（【](?P<tag>[^\]\)）】]+)[\]\)）】]\s*$")

    def start_scan(self, scan_paths: Iterable[Path]):
        self._scan_paths = {path.resolve() for path in scan_paths}
        self._existing_cache.clear()
        self._planned_cache.clear()

    def find_suspect(self, source: Path, target_folder: Path, detection: DetectionResult) -> Optional[SuspectCandidate]:
        if not self.cfg.suspect_check or detection.folder_name == UNCATEGORIZED_NAME:
            return None
        if target_folder.name == self.cfg.suspect_review_dir_name:
            return None

        source_key = self._title_key(source.name)
        if len(source_key) < 6:
            return None

        folder_key = target_folder.resolve()
        candidates = [
            *self._load_existing_candidates(folder_key, target_folder),
            *self._planned_cache.get(folder_key, []),
        ]
        source_path = source.resolve()

        for candidate in candidates:
            try:
                if candidate.compare_path.resolve() == source_path:
                    continue
            except OSError:
                continue
            if candidate.title_key == source_key:
                return candidate
        return None

    def remember_planned(self, target_folder: Path, source: Path, destination: Path):
        if not self.cfg.suspect_check:
            return
        title_key = self._title_key(source.name)
        if len(title_key) < 6:
            return
        folder_key = target_folder.resolve()
        bucket = self._planned_cache.setdefault(folder_key, [])
        bucket.append(
            SuspectCandidate(compare_path=source, display_path=destination, title_key=title_key)
        )

    def _load_existing_candidates(self, folder_key: Path, target_folder: Path) -> List[SuspectCandidate]:
        if folder_key in self._existing_cache:
            return self._existing_cache[folder_key]

        candidates: List[SuspectCandidate] = []
        if target_folder.exists() and target_folder.is_dir():
            for path in target_folder.iterdir():
                try:
                    resolved = path.resolve()
                except OSError:
                    continue
                if resolved in self._scan_paths:
                    continue
                if not path.is_file() or path.suffix.lower() not in self.cfg.allowed_extensions:
                    continue
                title_key = self._title_key(path.name)
                if len(title_key) < 6:
                    continue
                candidates.append(
                    SuspectCandidate(compare_path=path, display_path=path, title_key=title_key)
                )

        self._existing_cache[folder_key] = candidates
        return candidates

    def _title_key(self, filename: str) -> str:
        stem = Path(filename).stem
        text = re.sub(r"^\([a-zA-Z]*\d{2,4}\)\s*", "", stem).strip()
        text = re.sub(r"^(?:\[[^\]]+\]\s*)+", "", text).strip()
        text = self._strip_trailing_ignored_tags(text)
        text = text.replace("　", " ").strip()
        if not text:
            return ""

        key = text.casefold()
        key = re.sub(r"\s+", " ", key)
        key = re.sub(r"[\s\-_~～·・･•‧'\"`“”‘’.,，。:：;；!！?？/\\]+", "", key)
        return key.strip()

    def _strip_trailing_ignored_tags(self, text: str) -> str:
        current = text.strip()
        while current:
            match = self._tag_tail_pattern.search(current)
            if not match:
                break
            tag = match.group("tag").strip()
            if not self._is_ignored_tag(tag):
                break
            current = current[:match.start()].rstrip()
        return current

    def _is_ignored_tag(self, tag: str) -> bool:
        normalized = re.sub(r"\s+", "", tag).casefold()
        if not normalized:
            return False
        if normalized in self.cfg.suspect_ignored_tags_lower:
            return True
        return bool(
            re.search(
                r"(中国翻译|中国翻訳|汉化|漢化|翻译|翻訳|机翻|機翻|修正|無修正|无修正|dl版|^dl$|digital|english|eng|japanese|sample|mosaic|decensored)",
                normalized,
                re.IGNORECASE,
            )
        )


# ===================== 🧠 核心逻辑类 =====================

class ComicOrganizer:
    def __init__(self, config: OrganizerConfig):
        self.cfg = config
        self.script_dir = self._resolve_script_dir()
        self.session_manager = SessionManager(self.script_dir)
        self.logger = self._setup_logging(self.script_dir / LOG_FILE_NAME)
        self.folder_index = FolderIndex(config.source_dir, config.fuzzy_threshold)
        self.detector = SeriesNameDetector(config, self.folder_index)
        self.duplicate_detector = DuplicateDetector(config)
        self.suspect_detector = SuspectDetector(config)
        self.created_folders: Set[Path] = set()

    def run(self, scan_mode: ScanMode, dry_run: bool = True):
        session = self.session_manager.new_session(scan_mode=scan_mode, dry_run=dry_run)
        self.logger = self._setup_logging(session.log_file)
        self.logger.info(f"[start] mode={'dry-run' if dry_run else 'execute'} scan={scan_mode.value}")

        plans = self.build_move_plans(scan_mode)
        self.session_manager.save_plan(session, plans)
        self.session_manager.update_state(session, plans)
        self.logger.info(f"[plan] 移动计划数: {len(plans)}")

        for index, plan in enumerate(plans, 1):
            if index % 10 == 0 or index == len(plans):
                print(f"[progress] {index}/{len(plans)}", end="\r")
            self._apply_plan(plan, dry_run=dry_run)
            self.session_manager.update_state(session, plans)

        self._finalize(session, plans)

    def rollback(self, session_id: str = "latest"):
        session, plans = self.session_manager.load_session(session_id)
        self.logger = self._setup_logging(session.log_file)
        self.logger.info(f"[rollback-start] session={session.session_id}")

        for plan in reversed(plans):
            should_rollback = plan.status in {FileStatus.DONE, FileStatus.SUSPECT}
            if plan.status == FileStatus.DUPLICATE and plan.moved_to_duplicate_review:
                should_rollback = True
            if not should_rollback:
                continue
            self._rollback_single(plan)
            self.session_manager.update_state(session, plans)

        self._cleanup_empty_dirs(plans)
        self.logger.info(f"[rollback-done] session={session.session_id}")

    def list_history(self):
        sessions = self.session_manager.list_sessions()
        if not sessions:
            print("[history] 暂无历史会话")
            return

        print("[history] 历史会话：")
        for index, item in enumerate(sessions, 1):
            dry_run_tag = "DRY" if item["dry_run"] == "True" else "EXEC"
            print(
                f"{index}. {item['session_id']}  [{item['scan_mode']}]  [{dry_run_tag}]  {item['summary']}  {item['created_at']}"
            )

    def build_move_plans(self, scan_mode: ScanMode) -> List[MovePlan]:
        self.folder_index.refresh()
        files = self.scan_files(scan_mode)
        self.duplicate_detector.start_scan(files)
        self.suspect_detector.start_scan(files)
        self.logger.info(f"[scan] 待处理文件数: {len(files)}")

        staged_items: List[Tuple[Path, DetectionResult, Path, bool]] = []
        for src_path in files:
            detection = self.detector.detect(src_path.name)
            target_folder = self.cfg.source_dir / detection.folder_name
            is_in_target = src_path.parent.resolve() == target_folder.resolve()
            staged_items.append((src_path, detection, target_folder, is_in_target))

        plans: List[MovePlan] = []
        reserved_destinations: Set[Path] = set()

        for src_path, detection, target_folder, is_in_target in staged_items:
            if not is_in_target:
                continue
            plans.append(
                MovePlan(
                    source=src_path,
                    target_folder=target_folder,
                    destination=src_path,
                    detection=detection,
                    status=FileStatus.SKIPPED,
                )
            )
            self.duplicate_detector.remember_planned(target_folder, src_path, src_path)
            self.suspect_detector.remember_planned(target_folder, src_path, src_path)

        for src_path, detection, target_folder, is_in_target in staged_items:
            if is_in_target:
                continue

            duplicate_candidate = self.duplicate_detector.find_duplicate(src_path, target_folder)
            if duplicate_candidate:
                duplicate_path = duplicate_candidate.display_path
                review_folder = self._get_duplicate_review_folder(detection.folder_name)
                destination = self._get_unique_path(review_folder, src_path.name, reserved_destinations)
                reserved_destinations.add(destination)
                plans.append(
                    MovePlan(
                        source=src_path,
                        target_folder=review_folder,
                        destination=destination,
                        detection=detection,
                        status=FileStatus.DUPLICATE,
                        error=f"重复文件，已存在: {duplicate_path}",
                        duplicate_of=str(duplicate_path),
                    )
                )
                continue

            suspect_candidate = self.suspect_detector.find_suspect(src_path, target_folder, detection)
            if suspect_candidate:
                review_folder = self._get_suspect_review_folder(detection.folder_name)
                destination = self._get_unique_path(review_folder, src_path.name, reserved_destinations)
                reserved_destinations.add(destination)
                plans.append(
                    MovePlan(
                        source=src_path,
                        target_folder=review_folder,
                        destination=destination,
                        detection=detection,
                        status=FileStatus.SUSPECT,
                        error=f"疑似与现有文件为同作品不同版本: {suspect_candidate.display_path}",
                        suspect_of=str(suspect_candidate.display_path),
                    )
                )
                continue

            destination = self._get_unique_path(target_folder, src_path.name, reserved_destinations)
            reserved_destinations.add(destination)
            plans.append(
                MovePlan(
                    source=src_path,
                    target_folder=target_folder,
                    destination=destination,
                    detection=detection,
                )
            )
            self.duplicate_detector.remember_planned(target_folder, src_path, destination)
            self.suspect_detector.remember_planned(target_folder, src_path, destination)

        return plans

    def scan_files(self, scan_mode: ScanMode) -> List[Path]:
        if scan_mode == ScanMode.SAFE:
            return self._scan_safe()
        if scan_mode == ScanMode.REPAIR:
            return self._scan_repair()
        return self._scan_full()

    def _scan_safe(self) -> List[Path]:
        files = list(self._iter_direct_files(self.cfg.source_dir))
        uncategorized = self.cfg.source_dir / UNCATEGORIZED_NAME
        files.extend(self._iter_direct_files(uncategorized))
        return sorted(files, key=lambda path: str(path).casefold())

    def _scan_repair(self) -> List[Path]:
        files: List[Path] = []
        files.extend(self._iter_direct_files(self.cfg.source_dir))
        for child in self.cfg.source_dir.iterdir():
            if not child.is_dir() or self._is_reserved_root_dir(child):
                continue
            files.extend(self._iter_direct_files(child))
        return sorted({path.resolve(): path for path in files}.values(), key=lambda path: str(path).casefold())

    def _scan_full(self) -> List[Path]:
        files: List[Path] = []
        for path in self.cfg.source_dir.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in self.cfg.allowed_extensions:
                continue
            if self.cfg.suspect_review_dir_name in path.parts:
                continue
            if self.cfg.duplicate_review_dir_name in path.parts:
                continue
            files.append(path)
        return sorted(files, key=lambda path: str(path).casefold())

    def _iter_direct_files(self, directory: Path) -> Iterable[Path]:
        if not directory.exists() or not directory.is_dir():
            return []
        return [
            path
            for path in directory.iterdir()
            if path.is_file() and path.suffix.lower() in self.cfg.allowed_extensions
        ]

    def _apply_plan(self, plan: MovePlan, dry_run: bool):
        if plan.status == FileStatus.SKIPPED:
            self.logger.info(
                f"[skip][{plan.detection.match_type.value}] {plan.source} 已在目标目录: {plan.target_folder}"
            )
            return

        if plan.status == FileStatus.DUPLICATE:
            duplicate_target = plan.duplicate_of or "<unknown>"
            duplicate_review_folder = plan.target_folder
            duplicate_review_display = plan.destination
            tag = "[dry-run][duplicate]" if dry_run else "[duplicate]"
            if dry_run:
                self.logger.info(
                    f"{tag}[{plan.detection.match_type.value}] {plan.source.name} -> 重复区: {duplicate_review_display} (已存在相同文件: {duplicate_target})"
                )
                return

            try:
                folder_existed = duplicate_review_folder.exists()
                duplicate_review_folder.mkdir(parents=True, exist_ok=True)
                if not folder_existed:
                    self.created_folders.add(duplicate_review_folder)
                    plan.folder_created = True

                shutil.move(str(plan.source), str(plan.destination))
                plan.moved_to_duplicate_review = True
                self.logger.info(
                    f"{tag}[{plan.detection.match_type.value}] {plan.source.name} -> {plan.destination} (已存在相同文件: {duplicate_target})"
                )
            except Exception as exc:
                plan.status = FileStatus.FAILED
                plan.error = str(exc)
                self.logger.error(f"[error][duplicate] {plan.source.name}: {exc}")
            return

        if plan.status == FileStatus.SUSPECT:
            suspect_target = plan.suspect_of or "<unknown>"
            tag = "[dry-run][suspect]" if dry_run else "[suspect]"
            if dry_run:
                self.logger.info(
                    f"{tag}[{plan.detection.match_type.value}] {plan.source.name} -> 疑似重复待确认: {plan.target_folder} (参考: {suspect_target})"
                )
                return

            try:
                folder_existed = plan.target_folder.exists()
                plan.target_folder.mkdir(parents=True, exist_ok=True)
                if not folder_existed:
                    self.created_folders.add(plan.target_folder)
                    plan.folder_created = True

                shutil.move(str(plan.source), str(plan.destination))
                self.logger.info(
                    f"{tag}[{plan.detection.match_type.value}] {plan.source.name} -> {plan.destination} (参考: {suspect_target})"
                )
            except Exception as exc:
                plan.status = FileStatus.FAILED
                plan.error = str(exc)
                self.logger.error(f"[error][suspect] {plan.source.name}: {exc}")
            return

        target_display = plan.target_folder.name
        if plan.destination.name != plan.source.name:
            target_display = f"{target_display}\\{plan.destination.name}"

        if dry_run:
            self.logger.info(f"[dry-run][{plan.detection.match_type.value}] {plan.source.name} -> {target_display}")
            return

        try:
            folder_existed = plan.target_folder.exists()
            plan.target_folder.mkdir(parents=True, exist_ok=True)
            if not folder_existed:
                self.created_folders.add(plan.target_folder)
                plan.folder_created = True

            shutil.move(str(plan.source), str(plan.destination))
            plan.status = FileStatus.DONE
            self.logger.info(f"[move][{plan.detection.match_type.value}] {plan.source.name} -> {target_display}")
        except Exception as exc:
            plan.status = FileStatus.FAILED
            plan.error = str(exc)
            self.logger.error(f"[error] {plan.source.name}: {exc}")

    def _rollback_single(self, plan: MovePlan):
        if not plan.destination.exists():
            plan.error = f"回滚失败：目标文件不存在 {plan.destination}"
            self.logger.warning(plan.error)
            return

        rollback_target = plan.source
        rollback_target.parent.mkdir(parents=True, exist_ok=True)
        if rollback_target.exists():
            rollback_target = self._get_conflict_rollback_path(rollback_target)
            self.logger.warning(f"[rollback-conflict] 原路径已存在，改为恢复到: {rollback_target}")

        try:
            shutil.move(str(plan.destination), str(rollback_target))
            plan.status = FileStatus.ROLLED_BACK
            self.logger.info(f"[rollback] {plan.destination.name} -> {rollback_target}")
        except Exception as exc:
            plan.error = f"回滚失败：{exc}"
            self.logger.error(f"[rollback-error] {plan.destination.name}: {exc}")

    def _get_unique_path(self, target_folder: Path, filename: str, reserved_destinations: Set[Path]) -> Path:
        candidate = target_folder / filename
        if candidate not in reserved_destinations and not candidate.exists():
            return candidate

        stem = Path(filename).stem
        suffix = Path(filename).suffix
        counter = 1
        while True:
            candidate = target_folder / f"{stem} ({counter}){suffix}"
            if candidate not in reserved_destinations and not candidate.exists():
                return candidate
            counter += 1

    def _get_conflict_rollback_path(self, original_path: Path) -> Path:
        stem = original_path.stem
        suffix = original_path.suffix
        counter = 1
        while True:
            candidate = original_path.with_name(f"{stem} (rollback-conflict {counter}){suffix}")
            if not candidate.exists():
                return candidate
            counter += 1

    def _get_duplicate_review_folder(self, detected_folder_name: str) -> Path:
        review_root = self.cfg.source_dir / self.cfg.duplicate_review_dir_name
        return review_root / detected_folder_name

    def _get_suspect_review_folder(self, detected_folder_name: str) -> Path:
        review_root = self.cfg.source_dir / self.cfg.suspect_review_dir_name
        return review_root / detected_folder_name

    def _is_reserved_root_dir(self, directory: Path) -> bool:
        return directory.name in {self.cfg.suspect_review_dir_name, self.cfg.duplicate_review_dir_name}

    def _finalize(self, session: ExecutionSession, plans: List[MovePlan]):
        print("[separator] " + "=" * 30)
        done_count = sum(1 for plan in plans if plan.status == FileStatus.DONE)
        failed_count = sum(1 for plan in plans if plan.status == FileStatus.FAILED)
        skipped_count = sum(1 for plan in plans if plan.status == FileStatus.SKIPPED)
        duplicate_count = sum(1 for plan in plans if plan.status == FileStatus.DUPLICATE)
        suspect_count = sum(1 for plan in plans if plan.status == FileStatus.SUSPECT)
        rolled_back_count = sum(1 for plan in plans if plan.status == FileStatus.ROLLED_BACK)
        duplicate_moved_count = sum(1 for plan in plans if plan.moved_to_duplicate_review)
        total_count = len(plans)
        summary = (
            f"总计={total_count}, 成功={done_count}, 失败={failed_count}, "
            f"跳过={skipped_count}, 重复={duplicate_count}, 重复已移入重复区={duplicate_moved_count}, "
            f"疑似={suspect_count}, 已回滚={rolled_back_count}"
        )

        self.logger.info(f"[summary] {summary}")

        if session.dry_run:
            self.logger.info(
                f"[dry-run-done] done={done_count}, failed={failed_count}, skipped={skipped_count}, duplicate={duplicate_count}, duplicate_moved={duplicate_moved_count}, suspect={suspect_count}, rolled_back={rolled_back_count}, total={total_count}, session={session.session_dir}"
            )
            return

        self._cleanup_empty_dirs(plans)
        self.logger.info(
            f"[done] done={done_count}, failed={failed_count}, skipped={skipped_count}, duplicate={duplicate_count}, duplicate_moved={duplicate_moved_count}, suspect={suspect_count}, rolled_back={rolled_back_count}, total={total_count}, session={session.session_id}"
        )

    def _cleanup_empty_dirs(self, plans: List[MovePlan]):
        removable_dirs = {plan.target_folder for plan in plans if plan.folder_created}
        removable_dirs.add(self.cfg.source_dir / UNCATEGORIZED_NAME)
        removable_dirs.add(self.cfg.source_dir / self.cfg.suspect_review_dir_name)
        removable_dirs.add(self.cfg.source_dir / self.cfg.duplicate_review_dir_name)

        for directory in sorted(removable_dirs, key=lambda path: len(path.parts), reverse=True):
            if directory == self.cfg.source_dir:
                continue
            if not directory.exists() or not directory.is_dir():
                continue
            try:
                if any(directory.iterdir()):
                    continue
                directory.rmdir()
            except OSError:
                continue

    def _setup_logging(self, log_file: Path) -> logging.Logger:
        logger = logging.getLogger("comic_organizer")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.propagate = False

        formatter = logging.Formatter(
            fmt="[%(asctime)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger

    def _resolve_script_dir(self) -> Path:
        try:
            return Path(__file__).resolve().parent
        except NameError:
            return Path.cwd()


# ===================== 🚀 程序入口 =====================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="漫画压缩包/电子书整理脚本")
    parser.add_argument(
        "source_dir",
        nargs="?",
        default=str(DEFAULT_SOURCE_DIR),
        help=f"需要整理的根目录，默认: {DEFAULT_SOURCE_DIR}",
    )
    parser.add_argument(
        "--scan-mode",
        choices=[mode.value for mode in ScanMode],
        default=ScanMode.SAFE.value,
        help="扫描模式：safe / repair / full",
    )
    parser.add_argument("--dry-run", action="store_true", help="直接执行模拟测试")
    parser.add_argument("--execute", action="store_true", help="直接执行实战模式")
    parser.add_argument("--yes", action="store_true", help="实战模式跳过二次确认")
    parser.add_argument("--list-sessions", action="store_true", help="查看历史执行记录")
    parser.add_argument(
        "--rollback",
        metavar="SESSION_ID",
        help="回滚指定会话，传 latest 表示最近一次实战",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.85,
        help="模糊匹配阈值，范围 0.0-1.0，默认 0.85",
    )
    return parser


def confirm_execute(scan_mode: ScanMode) -> bool:
    if scan_mode == ScanMode.FULL:
        print("[warning] 全量重扫会递归扫描整个目录树，可能影响深层手工整理结构。")
    elif scan_mode == ScanMode.REPAIR:
        print("[warning] 修历史归档会扫描一级作者目录，请先确认你理解回滚方式。")
    return input("[confirm] 确认执行？(yes/no): ").strip().lower() == "yes"


def interactive_menu() -> str:
    print("1. 安全整理（只扫根目录 + 未分类）")
    print("2. 修历史归档（再扫一级作者目录）")
    print("3. 全量重扫（递归整个目录树）")
    print("4. 回滚上一次执行")
    print("5. 查看历史执行记录")
    print("0. 退出")
    return input("[select] 请选择: ").strip()


def interactive_run(organizer: ComicOrganizer) -> int:
    choice = interactive_menu()
    if choice == "0":
        print("[exit] 已退出")
        return 0
    if choice == "4":
        organizer.rollback("latest")
        return 0
    if choice == "5":
        organizer.list_history()
        return 0

    mode_map = {
        "1": ScanMode.SAFE,
        "2": ScanMode.REPAIR,
        "3": ScanMode.FULL,
    }
    scan_mode = mode_map.get(choice)
    if not scan_mode:
        print("[error] 无效选择")
        return 1

    print("1. 模拟测试")
    print("2. 实战执行")
    run_choice = input("[select] 请选择执行方式: ").strip()
    if run_choice == "1":
        organizer.run(scan_mode=scan_mode, dry_run=True)
        return 0
    if run_choice == "2":
        if confirm_execute(scan_mode):
            organizer.run(scan_mode=scan_mode, dry_run=False)
        else:
            print("[exit] 已取消执行")
        return 0

    print("[error] 无效选择")
    return 1


def main() -> int:
    args = build_parser().parse_args()

    if not 0.0 <= args.threshold <= 1.0:
        print("[error] 模糊匹配阈值必须在 0.0 到 1.0 之间")
        return 1

    source_dir = Path(args.source_dir)
    config = OrganizerConfig(source_dir=source_dir, fuzzy_threshold=args.threshold)
    organizer = ComicOrganizer(config)

    if args.list_sessions:
        organizer.list_history()
        return 0

    if args.rollback:
        organizer.rollback(args.rollback)
        return 0

    if not source_dir.exists():
        print("[error] 路径不存在")
        return 1
    if not source_dir.is_dir():
        print("[error] 路径不是文件夹")
        return 1

    print(f"Comic Organizer V4.0 (当前路径: {config.source_dir})")

    if not args.dry_run and not args.execute:
        return interactive_run(organizer)

    dry_run = not args.execute
    scan_mode = ScanMode(args.scan_mode)
    if not dry_run and not args.yes and not confirm_execute(scan_mode):
        print("[exit] 已取消执行")
        return 0

    organizer.run(scan_mode=scan_mode, dry_run=dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
