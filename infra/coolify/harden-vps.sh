#!/usr/bin/env bash
# =============================================================================
# Arthala ID — by Robit Al Hazmi
# Idempotent 1 vCPU / 2 GB Coolify VPS Hardening & Swap Provisioning Script
# Task 0.5 (docs/Task.md & docs/Walkthrough.md Step 3)
#
# Usage on your Coolify VPS:
#   sudo bash infra/coolify/harden-vps.sh
# =============================================================================
set -euo pipefail

SWAP_FILE="${SWAP_FILE:-/swapfile}"
SWAP_SIZE="${SWAP_SIZE:-2G}"
MIN_SWAP_MIB="${MIN_SWAP_MIB:-1900}"
SYSCTL_FILE="${SYSCTL_FILE:-/etc/sysctl.d/99-arthala-coolify.conf}"

echo "=== [1/4] Checking active Linux swap (target >= ${MIN_SWAP_MIB} MiB) ==="
current_swap_kib="$(awk '/^SwapTotal:/ {print $2}' /proc/meminfo)"
current_swap_mib="$(( current_swap_kib / 1024 ))"

if (( current_swap_mib >= MIN_SWAP_MIB )); then
    echo "Swap already active: ${current_swap_mib} MiB (>= ${MIN_SWAP_MIB} MiB). Skipping allocation."
else
    echo "Active swap is ${current_swap_mib} MiB. Provisioning ${SWAP_SIZE} at ${SWAP_FILE}..."
    if [[ ! -f "${SWAP_FILE}" ]]; then
        if command -v fallocate >/dev/null 2>&1; then
            fallocate -l "${SWAP_SIZE}" "${SWAP_FILE}"
        else
            dd if=/dev/zero of="${SWAP_FILE}" bs=1M count=2048 status=progress
        fi
    fi
    chmod 600 "${SWAP_FILE}"
    mkswap "${SWAP_FILE}"
    swapon "${SWAP_FILE}"
    if ! grep -qE "^${SWAP_FILE}[[:space:]]+" /etc/fstab; then
        echo "${SWAP_FILE} none swap sw 0 0" >> /etc/fstab
    fi
    echo "Enabled ${SWAP_SIZE} swap at ${SWAP_FILE}."
fi

echo "=== [2/4] Applying kernel memory tuning (${SYSCTL_FILE}) ==="
cat <<'EOF' > "${SYSCTL_FILE}"
# Arthala ID — 1 vCPU / 2 GB Coolify VPS memory tuning
vm.swappiness=20
vm.vfs_cache_pressure=50
EOF
sysctl --system >/dev/null

echo "=== [3/4] Verifying Docker daemon & Coolify core containers ==="
if command -v docker >/dev/null 2>&1; then
    docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
    echo "--- Container RAM usage ---"
    docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'
else
    echo "WARNING: docker binary not found on PATH."
fi

echo "=== [4/4] Host Memory & Swap Summary ==="
free -h
swapon --show
echo "Coolify VPS hardening complete."
