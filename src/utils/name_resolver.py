"""Name resolution and standardization utilities.

Handles mapping between different name formats across data sources
(FPL API, Understat, vaastav). Supports fuzzy matching with
Levenshtein distance, mapping tables, and confidence scoring.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MAPPING_FILE = Path("data/raw/name_mapping.json")

# Known name variations mapping (source_name -> canonical_name)
KNOWN_VARIATIONS: dict[str, str] = {
    # Goalkeepers
    "Alisson Ramses Becker": "Alisson",
    "Alisson": "Alisson",
    "Ederson Santana de Moraes": "Ederson",
    "Kepa Arrizabalaga": "Kepa",
    "David de Gea Quintana": "David de Gea",
    # Outfield
    "Erling Haaland": "Erling Haaland",
    "Erling Braut Haaland": "Erling Haaland",
    "Mohamed Salah Hamed Mahrous Ghaly": "Mohamed Salah",
    "Mohamed Salah": "Mohamed Salah",
    "Son Heung-Min": "Son Heung-min",
    "Son Heung min": "Son Heung-min",
    "Pascal Gro\u00df": "Pascal Gross",
    "Jo\u00e3o Cancelo": "Joao Cancelo",
    "Jo\u00e3o Pedro": "Joao Pedro",
    "Matheus Luiz Nunes": "Matheus Nunes",
    "Gabriel dos Santos Magalh\u00e3es": "Gabriel Magalh\u00e3es",
    "Gabriel dos Santos Magalhaes": "Gabriel Magalh\u00e3es",
    "Gabriel Fernando de Jesus": "Gabriel Jesus",
    "Richarlison de Andrade": "Richarlison",
    "Philippe Coutinho Correia": "Philippe Coutinho",
    "Bernardo Mota Veiga de Carvalho e Silva": "Bernardo Silva",
    "Jos\u00e9 Sa": "Jose Sa",
    "José Malheiro de Sá": "Jose Sa",
    "Diogo Jos\u00e9 Teixeira da Silva": "Diogo Jota",
    "Diogo Jota": "Diogo Jota",
    # 2025-26 known name additions (Understat -> FPL/known_name format)
    # These fix the unmapped players issue
    "Toti": "Toti Gomes",
    "Hee-Chan Hwang": "Hwang Hee-Chan",
    # Manual mapping overrides for players that don't auto-match
    # Casemiro variants
    "Casemiro": "Carlos Henrique Casimiro",
    # Rodri variants
    "Rodri": "Rodrigo Hernandez",
    "Rodrigo Hernández Cascante": "Rodrigo Hernandez",
    # Diogo Jota variants
    "Diogo Jota": "Diogo Teixeira da Silva",
    "Diogo José Teixeira da Silva": "Diogo Teixeira da Silva",
    # Beto variants
    "Beto": "Norberto Bercique Gomes Betuncal",
    "Norberto Bercique Gomes": "Norberto Bercique Gomes Betuncal",
    # Amad Diallo
    "Amad Diallo Traore": "Amad Diallo",
    # Son Heung-Min
    "Son Heung-Min": "Son Heung-min",
    # Igor Julio
    "Igor Julio": "Igor Julio dos Santos de Paulo",
    # Additional players that need explicit mapping
    "Jota Silva": "João Pedro Ferreira da Silva",
    # More players that need explicit mapping for 2025-26
    "Alisson": "Alisson Becker",
    "Reinildo": "Reinildo Mandava",
    "Sávio": "Sávio Moreira de Oliveira",
    # Additional historical season mappings
    # Antony
    "Antony": "Antony Matheus dos Santos",
    # Bernardo Silva
    "Bernardo Silva": "Bernardo Veiga de Carvalho e Silva",
    # Danilo
    "Danilo": "Danilo dos Santos de Oliveira",
    # Douglas Luiz
    "Douglas Luiz": "Douglas Luiz Soares de Paulo",
    # Diogo Dalot
    "Diogo Dalot": "José Diogo Dalot Teixeira",
    # Fred
    "Fred": "Frederico Rodrigues de Paula Santos",
    # Joelinton
    "Joelinton": "Joelinton Cássio Apolinário de Lira",
    # Jonny
    "Jonny": "Jonny Evans",
    # Rúben Dias - UNDERSTAT uses short name, VAASTAV uses full legal name
    # Must map to full legal name so both standardize to same thing
    "Rúben Dias": "Ruben Santos Gato Alves Dias",
    # Rúben Neves - same issue (but he plays for Wolves, different player)
    "Rúben Neves": "Ruben Santos Gato Alves Dias",
    # Son Heung-Min - UNDERSTAT uses "Son Heung-Min", VAASTAV uses "Heung-Min Son"
    "Son Heung-Min": "Heung-Min Son",
    "Son Heung min": "Heung-Min Son",
    # Felipe
    "Felipe": "Felipe Augusto de Almeida Monteiro",
    # Willian
    "Willian": "Willian Borges da Silva",
    # Neto
    "Neto": "Pedro Lomba Neto",
    # Lyanco
    "Lyanco": "Lyanco Evangelista Silveira Neves Vojnovic",
    # Matheus Cunha
    "Matheus Cunha": "Matheus Santos Carneiro Da Cunha",
    # Murillo
    "Murillo": "Murillo Santiago Costa dos Santos",
    # João Pedro
    "João Pedro": "João Pedro Cavaco Cancelo",
    # Takehiro Tomiyasu
    "Takehiro Tomiyasu": "Tomiyasu Takehiro",
    # Sávio
    "Sávio": "Sávio 'Savinho' Moreia de Oliveira",
    "Ao Tanaka": "Tanaka",
    "Jota Silva": "Jota",
    "Ben White": "White",
    "Wataru Endo": "Endo",
    "Kaoru Mitoma": "Mitoma",
    # Lucas Paquetá variants
    "Lucas Paquetá": "Lucas Tolentino Coelho de Lima",
    # Igor Julio variants
    "Igor Julio": "Igor Julio dos Santos de Paulo",
    # Fernando López variants
    "Fernando López": "Fer López González",
    # Alejandro Jiménez variants
    "Alejandro Jiménez": "Álex Jiménez Sánchez",
    # Jair variants
    "Jair": "Jair Paula da Cunha Filho",
    # Hamed Junior Traore variants
    "Hamed Junior Traore": "Hamed Traorè",
    # Additional common variations
    "Emerson Palmieri dos Santos": "Emerson",
    "Emerson Aparecido Leite de Souza Junior": "Emerson",
    "José Sá": "Jose Sa",
    "Bernardo Silva": "Bernardo Silva",
    "Gabriel": "Gabriel Magalhães",
    "Son Heung-min": "Son Heung-min",
    # Remaining 2025-26 unmatched players
    "Ben White": "Benjamin White",
    "Wataru Endo": "Endo Wataru",
    "Jair": "Jair Cunha",
    "Jota Silva": "Jota",
    # Team name variations
    "Man City": "Manchester City",
    "Manchester City": "Manchester City",
    "Man Utd": "Manchester United",
    "Manchester United": "Manchester United",
    "Man United": "Manchester United",
    "Spurs": "Tottenham",
    "Tottenham Hotspur": "Tottenham",
    "Tottenham": "Tottenham",
    "Wolves": "Wolverhampton Wanderers",
    "Wolverhampton": "Wolverhampton Wanderers",
    "Wolverhampton Wanderers": "Wolverhampton Wanderers",
    "Newcastle United": "Newcastle",
    "Newcastle": "Newcastle",
    "West Ham United": "West Ham",
    "West Ham": "West Ham",
    "AFC Bournemouth": "Bournemouth",
    "Bournemouth": "Bournemouth",
    "Nott'm Forest": "Nottingham Forest",
    "Nottingham Forest": "Nottingham Forest",
    "Sheffield Utd": "Sheffield United",
    "Sheffield United": "Sheffield United",
}


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein distance between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Edit distance (number of insertions, deletions, substitutions).
    """
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row: list[int] = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def _confidence_score(distance: int, max_len: int) -> float:
    """Convert Levenshtein distance to a confidence score [0, 1].

    Args:
        distance: Levenshtein distance.
        max_len: Maximum length of the two strings.

    Returns:
        Confidence score between 0.0 and 1.0.
    """
    if max_len == 0:
        return 1.0
    return max(0.0, 1.0 - (distance / max_len))


def standardize_name(name: str) -> str:
    """Standardize a player name to 'First Last' format.

    Handles:
    - Known variations mapping (e.g., "Toti" -> "Toti Gomes")
    - "Last, First" -> "First Last"
    - Parenthetical suffixes like "(Captain)", "[Injured]"
    - Accented characters (normalized to ASCII)
    - Title casing

    Args:
        name: Raw player name string.

    Returns:
        Standardized name in title case.
    """
    name = name.strip()

    # First check known variations - use this to resolve common mismatches
    # This maps Understat names to FPL/known_name format
    if name in KNOWN_VARIATIONS:
        name = KNOWN_VARIATIONS[name]
        # Continue to standardize (don't return early) to handle full legal names

    # Remove parenthetical and bracketed suffixes
    name = re.sub(r"\s*\([^)]*\)", "", name)
    name = re.sub(r"\s*\[[^\]]*\]", "", name)

    # Handle "Last, First" format
    if "," in name:
        parts = name.split(",", 1)
        if len(parts) == 2:
            name = f"{parts[1].strip()} {parts[0].strip()}"

    # Normalize accented characters to ASCII
    import unicodedata

    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))

    # Title case
    name = name.title()

    return name


def fuzzy_match_name(
    name: str,
    candidates: list[str],
    threshold: float = 0.8,
) -> tuple[str | None, float]:
    """Find the best fuzzy match for a name among candidates.

    Uses Levenshtein distance to compute similarity scores.

    Args:
        name: Name to match.
        candidates: List of candidate names.
        threshold: Minimum confidence score to accept a match.

    Returns:
        Tuple of (matched_name or None, confidence_score).
    """
    norm_name = standardize_name(name)
    best_match: str | None = None
    best_score = 0.0

    for candidate in candidates:
        norm_candidate = standardize_name(candidate)
        distance = _levenshtein_distance(norm_name, norm_candidate)
        max_len = max(len(norm_name), len(norm_candidate))
        score = _confidence_score(distance, max_len)

        if score > best_score:
            best_score = score
            best_match = candidate

    if best_score >= threshold and best_match is not None:
        return best_match, best_score
    return None, best_score


def build_name_mapping(
    source_names: list[str],
    target_names: list[str],
    threshold: float = 0.8,
) -> dict[str, tuple[str, float]]:
    """Build a mapping from source names to target names.

    Uses known variations first, then fuzzy matching for remaining names.

    Args:
        source_names: List of names from source data.
        target_names: List of canonical names.
        threshold: Minimum confidence score for fuzzy matches.

    Returns:
        Dict mapping source names to (target_name, confidence_score).
    """
    mapping: dict[str, tuple[str, float]] = {}
    target_set = set(target_names)
    target_lower = {n.lower(): n for n in target_names}

    for src in source_names:
        norm_src = standardize_name(src)

        # 1. Check known variations
        if src in KNOWN_VARIATIONS:
            canonical = KNOWN_VARIATIONS[src]
            if canonical in target_set:
                mapping[src] = (canonical, 1.0)
                continue

        # 2. Exact match on normalized name
        if norm_src.lower() in target_lower:
            mapping[src] = (target_lower[norm_src.lower()], 1.0)
            continue

        # 3. Fuzzy match
        match, score = fuzzy_match_name(src, target_names, threshold)
        if match is not None:
            mapping[src] = (match, score)
        elif score > 0.0:
            # Fuzzy match found but below threshold - still record it
            # for logging purposes, but mark as unresolved
            mapping[src] = (src, score)
        else:
            mapping[src] = (src, 0.0)  # No match found

    return mapping


def resolve_names(
    source_names: list[str],
    target_names: list[str],
    threshold: float = 0.8,
    log_to_mlflow: bool = True,
) -> tuple[dict[str, str], dict[str, float]]:
    """Resolve source names to canonical target names.

    Args:
        source_names: Names to resolve.
        target_names: Canonical target names.
        threshold: Minimum confidence for fuzzy matches.
        log_to_mlflow: Whether to log results to MLflow.

    Returns:
        Tuple of (resolved_mapping, confidence_scores).
    """
    raw_mapping = build_name_mapping(source_names, target_names, threshold)

    resolved: dict[str, str] = {}
    confidence: dict[str, float] = {}
    low_confidence: list[dict[str, Any]] = []

    for src, (target, score) in raw_mapping.items():
        resolved[src] = target
        confidence[src] = score

        if score < threshold and score > 0.0:
            low_confidence.append(
                {"source": src, "target": target, "confidence": round(score, 3)}
            )

    if low_confidence:
        logger.warning(
            "%d low-confidence name matches (threshold=%.2f): %s",
            len(low_confidence),
            threshold,
            low_confidence[:5],
        )

    if log_to_mlflow:
        _log_name_resolution(confidence, low_confidence)

    return resolved, confidence


def _log_name_resolution(
    confidence: dict[str, float],
    low_confidence: list[dict[str, Any]],
) -> None:
    """Log name resolution results to MLflow.

    Args:
        confidence: Dict mapping source names to confidence scores.
        low_confidence: List of low-confidence match details.
    """
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping name resolution logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_data_cleaning")
        with mlflow.start_run(run_name="name_resolution"):
            total = len(confidence)
            exact = sum(1 for s in confidence.values() if s == 1.0)
            mlflow.log_param("total_names", total)
            mlflow.log_param("exact_matches", exact)
            mlflow.log_param("fuzzy_matches", total - exact)
            mlflow.log_param("low_confidence_count", len(low_confidence))
            avg_conf = sum(confidence.values()) / total if total > 0 else 0.0
            mlflow.log_metric("avg_confidence", avg_conf)
            if low_confidence:
                mlflow.log_param("low_confidence_details", str(low_confidence[:10]))
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log name resolution to MLflow: %s", e)
