#!/bin/sh

describe_run_args() {
    if [ "$#" -gt 0 ]; then
        printf '%s' "$*"
    else
        printf '%s' "default previous month"
    fi
}

is_vahan_headless_disabled() {
    case "${VAHAN_HEADLESS:-true}" in
        0|false|FALSE|False|no|NO|No|off|OFF|Off)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

resolve_xvfb_run_bin() {
    if [ -n "${XVFB_RUN_BIN:-}" ]; then
        printf '%s' "${XVFB_RUN_BIN}"
        return 0
    fi

    command -v xvfb-run 2>/dev/null || true
}

run_selenium_step() {
    if is_vahan_headless_disabled; then
        xvfb_run_bin="$(resolve_xvfb_run_bin)"
        if [ -z "${xvfb_run_bin}" ]; then
            printf '%s - ERROR - VAHAN_HEADLESS=%s requires xvfb-run, but it was not found in PATH.\n' \
                "$(date '+%Y-%m-%d %H:%M:%S')" "${VAHAN_HEADLESS}" >&2
            return 1
        fi
        "${xvfb_run_bin}" -a "$@"
        return $?
    fi

    "$@"
}

send_failure_alert() {
    if [ -z "${PROJECT_ROOT:-}" ]; then
        printf '%s - WARNING - PROJECT_ROOT is not set. Skipping Google Chat alert.\n' \
            "$(date '+%Y-%m-%d %H:%M:%S')" >&2
        return 1
    fi

    if [ ! -f "${PROJECT_ROOT}/ops/send_chat_alert.py" ]; then
        printf '%s - WARNING - Google Chat alert script not found at %s.\n' \
            "$(date '+%Y-%m-%d %H:%M:%S')" "${PROJECT_ROOT}/ops/send_chat_alert.py" >&2
        return 1
    fi

    if [ -n "${ETL_ALERT_DETAILS:-}" ] && [ -n "${ETL_ALERT_DETAILS_FILE:-}" ]; then
        python3 "${PROJECT_ROOT}/ops/send_chat_alert.py" \
            --pipeline "${ETL_PIPELINE_NAME:-unknown}" \
            --status FAILED \
            --run-label "${RUN_LABEL:-unknown}" \
            --step "${CURRENT_STEP:-unknown}" \
            --exit-code "$1" \
            --hostname "$(hostname 2>/dev/null || printf '%s' 'unknown-host')" \
            --headless "${VAHAN_HEADLESS:-unknown}" \
            --log-file "${ETL_LOG_FILE:-}" \
            --details "${ETL_ALERT_DETAILS}" \
            --details-file "${ETL_ALERT_DETAILS_FILE}"
        return $?
    fi

    if [ -n "${ETL_ALERT_DETAILS:-}" ]; then
        python3 "${PROJECT_ROOT}/ops/send_chat_alert.py" \
            --pipeline "${ETL_PIPELINE_NAME:-unknown}" \
            --status FAILED \
            --run-label "${RUN_LABEL:-unknown}" \
            --step "${CURRENT_STEP:-unknown}" \
            --exit-code "$1" \
            --hostname "$(hostname 2>/dev/null || printf '%s' 'unknown-host')" \
            --headless "${VAHAN_HEADLESS:-unknown}" \
            --log-file "${ETL_LOG_FILE:-}" \
            --details "${ETL_ALERT_DETAILS}"
        return $?
    fi

    if [ -n "${ETL_ALERT_DETAILS_FILE:-}" ]; then
        python3 "${PROJECT_ROOT}/ops/send_chat_alert.py" \
            --pipeline "${ETL_PIPELINE_NAME:-unknown}" \
            --status FAILED \
            --run-label "${RUN_LABEL:-unknown}" \
            --step "${CURRENT_STEP:-unknown}" \
            --exit-code "$1" \
            --hostname "$(hostname 2>/dev/null || printf '%s' 'unknown-host')" \
            --headless "${VAHAN_HEADLESS:-unknown}" \
            --log-file "${ETL_LOG_FILE:-}" \
            --details-file "${ETL_ALERT_DETAILS_FILE}"
        return $?
    fi

    python3 "${PROJECT_ROOT}/ops/send_chat_alert.py" \
        --pipeline "${ETL_PIPELINE_NAME:-unknown}" \
        --status FAILED \
        --run-label "${RUN_LABEL:-unknown}" \
        --step "${CURRENT_STEP:-unknown}" \
        --exit-code "$1" \
        --hostname "$(hostname 2>/dev/null || printf '%s' 'unknown-host')" \
        --headless "${VAHAN_HEADLESS:-unknown}" \
        --log-file "${ETL_LOG_FILE:-}"
}

send_success_alert() {
    if [ -z "${PROJECT_ROOT:-}" ]; then
        printf '%s - WARNING - PROJECT_ROOT is not set. Skipping Google Chat alert.\n' \
            "$(date '+%Y-%m-%d %H:%M:%S')" >&2
        return 1
    fi

    if [ ! -f "${PROJECT_ROOT}/ops/send_chat_alert.py" ]; then
        printf '%s - WARNING - Google Chat alert script not found at %s.\n' \
            "$(date '+%Y-%m-%d %H:%M:%S')" "${PROJECT_ROOT}/ops/send_chat_alert.py" >&2
        return 1
    fi

    if [ -n "${ETL_SUCCESS_DETAILS:-}" ]; then
        python3 "${PROJECT_ROOT}/ops/send_chat_alert.py" \
            --pipeline "${ETL_PIPELINE_NAME:-unknown}" \
            --status SUCCESS \
            --run-label "${RUN_LABEL:-unknown}" \
            --step "${CURRENT_STEP:-completed}" \
            --exit-code 0 \
            --hostname "$(hostname 2>/dev/null || printf '%s' 'unknown-host')" \
            --headless "${VAHAN_HEADLESS:-unknown}" \
            --log-file "${ETL_LOG_FILE:-}" \
            --details "${ETL_SUCCESS_DETAILS}"
        return $?
    fi

    python3 "${PROJECT_ROOT}/ops/send_chat_alert.py" \
        --pipeline "${ETL_PIPELINE_NAME:-unknown}" \
        --status SUCCESS \
        --run-label "${RUN_LABEL:-unknown}" \
        --step "${CURRENT_STEP:-completed}" \
        --exit-code 0 \
        --hostname "$(hostname 2>/dev/null || printf '%s' 'unknown-host')" \
        --headless "${VAHAN_HEADLESS:-unknown}" \
        --log-file "${ETL_LOG_FILE:-}"
}

etl_failure_trap_handler() {
    exit_code="$1"

    trap - ERR

    printf '%s - ERROR - ETL failed pipeline=%s run=%s step=%s exit_code=%s\n' \
        "$(date '+%Y-%m-%d %H:%M:%S')" \
        "${ETL_PIPELINE_NAME:-unknown}" \
        "${RUN_LABEL:-unknown}" \
        "${CURRENT_STEP:-unknown}" \
        "${exit_code}" >&2

    set +e
    send_failure_alert "${exit_code}"
    alert_exit_code="$?"
    set -e

    if [ "${alert_exit_code}" -ne 0 ]; then
        printf '%s - WARNING - Google Chat failure alert could not be delivered for pipeline=%s.\n' \
            "$(date '+%Y-%m-%d %H:%M:%S')" \
            "${ETL_PIPELINE_NAME:-unknown}" >&2
    fi

    exit "${exit_code}"
}

install_failure_alert_trap() {
    trap 'etl_failure_trap_handler "$?"' ERR
}
