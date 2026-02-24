#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/export_to_community.sh --target-dir /path/to/datachat-community [--dry-run] [--no-delete]

Options:
  --target-dir PATH   Required. Local checkout path of datachat-community.
  --dry-run           Show what would change without writing files.
  --no-delete         Do not delete files in target that do not exist in source.
  --help              Show this help message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
EXCLUDE_FILE="${SOURCE_ROOT}/.community-export-ignore"

TARGET_DIR=""
DRY_RUN=0
DELETE_MODE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-dir)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --no-delete)
      DELETE_MODE=0
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${TARGET_DIR}" ]]; then
  echo "Error: --target-dir is required." >&2
  usage
  exit 1
fi

if [[ ! -d "${SOURCE_ROOT}/.git" ]]; then
  echo "Error: source root does not look like a git repository: ${SOURCE_ROOT}" >&2
  exit 1
fi

if [[ ! -f "${SOURCE_ROOT}/pyproject.toml" ]]; then
  echo "Error: pyproject.toml not found in ${SOURCE_ROOT}" >&2
  exit 1
fi

if [[ ! -f "${EXCLUDE_FILE}" ]]; then
  echo "Error: missing exclude file: ${EXCLUDE_FILE}" >&2
  exit 1
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "Error: rsync is required but not installed." >&2
  exit 1
fi

if [[ ! -d "${TARGET_DIR}" ]]; then
  echo "Error: target directory does not exist: ${TARGET_DIR}" >&2
  exit 1
fi

if [[ ! -d "${TARGET_DIR}/.git" ]]; then
  echo "Error: target directory is not a git repository: ${TARGET_DIR}" >&2
  exit 1
fi

TARGET_DIR="$(cd "${TARGET_DIR}" && pwd)"

echo "Source: ${SOURCE_ROOT}"
echo "Target: ${TARGET_DIR}"
echo "Mode:   $([[ ${DRY_RUN} -eq 1 ]] && echo 'dry-run' || echo 'write')"
echo "Delete: $([[ ${DELETE_MODE} -eq 1 ]] && echo 'enabled' || echo 'disabled')"

RSYNC_ARGS=(
  -av
  --exclude-from="${EXCLUDE_FILE}"
  --filter=':- .gitignore'
)

if [[ ${DELETE_MODE} -eq 1 ]]; then
  RSYNC_ARGS+=(--delete)
fi

if [[ ${DRY_RUN} -eq 1 ]]; then
  RSYNC_ARGS+=(-n)
fi

RSYNC_ARGS+=("${SOURCE_ROOT}/" "${TARGET_DIR}/")

echo
echo "Running export..."
rsync "${RSYNC_ARGS[@]}"

if [[ ${DRY_RUN} -eq 1 ]]; then
  echo
  echo "Dry run complete."
  exit 0
fi

echo
echo "Running sanity checks..."

required_paths=(
  "README.md"
  "backend"
  "frontend"
  "docs"
  "scripts"
)

excluded_paths=(
  "docs/ARCHITECTURE_DYNAMIC_DATA_AGENT.md"
  "docs/GTM_90_DAY_PLAN.md"
  "docs/LEVELS.md"
  "docs/PRD.md"
  "docs/ROADMAP.md"
  "docs/OSS_SPLIT_CHECKLIST.md"
  "docs/specs"
  "docs/templates/COMMUNITY_REPO_README_TEMPLATE.md"
  "docs/finance/FINANCE_WORKFLOW_VALUE_PROOF.md"
  "reports"
  "eval"
  "workspace_demo"
  ".env"
  ".coverage"
  ".ruff_cache"
  "chroma_data"
  "datachat.egg-info"
)

for path in "${required_paths[@]}"; do
  if [[ ! -e "${TARGET_DIR}/${path}" ]]; then
    echo "Sanity check failed: missing required path '${path}' in target." >&2
    exit 1
  fi
done

for path in "${excluded_paths[@]}"; do
  if [[ -e "${TARGET_DIR}/${path}" ]]; then
    echo "Sanity check failed: excluded path still exists '${path}' in target." >&2
    exit 1
  fi
done

echo "Sanity checks passed."
echo "Export completed successfully."
