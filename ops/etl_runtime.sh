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
