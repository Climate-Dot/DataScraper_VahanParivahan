#!/bin/bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DBT_BIN="${DBT_BIN:-dbt}"
DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-${PROJECT_ROOT}/climate_dot_dbt}"

UNIT_TEST_MODULES=(
    tests.test_runtime_support
    tests.test_shared_etl_support
    tests.test_missing_file_recovery
    tests.test_selenium_logging
    tests.test_rto_mapping_refresh
    tests.test_schema_regression
    tests.test_chat_alerts
    tests.test_dbt_contracts
    tests.test_migration_contract
)

should_run_dbt_parse() {
    case "${RUN_DBT_PARSE:-0}" in
        1|true|TRUE|True|yes|YES|Yes|on|ON|On)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

log_step() {
    printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

run_shell_checks() {
    log_step "Checking shell syntax"
    bash -n "${PROJECT_ROOT}/oem_data_etl.sh" \
        "${PROJECT_ROOT}/state_ev_data_etl.sh" \
        "${PROJECT_ROOT}/rto_ev_data_etl.sh" \
        "${PROJECT_ROOT}/ops/run_repo_checks.sh"
    sh -n "${PROJECT_ROOT}/ops/etl_runtime.sh"
}

run_unit_tests() {
    log_step "Running Python unit tests"
    "${PYTHON_BIN}" -m unittest "${UNIT_TEST_MODULES[@]}"
}

run_dbt_parse() {
    if ! should_run_dbt_parse; then
        return 0
    fi

    if ! command -v "${DBT_BIN}" >/dev/null 2>&1; then
        printf 'dbt command not found: %s\n' "${DBT_BIN}" >&2
        return 1
    fi

    if [ -z "${DBT_PROFILES_DIR:-}" ]; then
        printf 'DBT_PROFILES_DIR must be set when RUN_DBT_PARSE is enabled.\n' >&2
        return 1
    fi

    log_step "Running dbt parse"
    "${DBT_BIN}" parse \
        --project-dir "${DBT_PROJECT_DIR}" \
        --profiles-dir "${DBT_PROFILES_DIR}"
}

main() {
    cd "${PROJECT_ROOT}"

    run_shell_checks
    run_unit_tests
    run_dbt_parse
}

main "$@"
