#!/bin/bash

# Resource Usage Monitor for Load Testing
# Single snapshot of Kubernetes resources - use with 'watch' command
# Usage: watch -n 5 ./monitor_resources.sh

set -e

# Common utility functions
calculate_percentage() {
    local usage="$1"
    local limit="$2"
    
    if [[ -z "$usage" || "$usage" == "N/A" || -z "$limit" || "$limit" == "N/A" ]]; then
        echo "N/A"
        return
    fi
    
    # Extract numbers and units
    local usage_num=$(echo "$usage" | sed 's/[^0-9]//g')
    local usage_unit=$(echo "$usage" | sed 's/[0-9]//g')
    local limit_num=$(echo "$limit" | sed 's/[^0-9]//g')
    local limit_unit=$(echo "$limit" | sed 's/[0-9]//g')
    
    # If units match, calculate directly
    if [[ "$usage_unit" == "$limit_unit" && -n "$usage_num" && -n "$limit_num" && "$limit_num" -gt 0 ]]; then
        local percentage=$(( usage_num * 100 / limit_num ))
        echo "${percentage}%"
    else
        echo "N/A"
    fi
}

print_header() {
    printf "%-30s %-12s %-8s %-12s %-12s %-8s %-12s\n" "NAME" "CPU(cores)" "%CPU" "CPU-LIMIT" "MEMORY" "%MEM" "MEM-LIMIT"
    printf "%-30s %-12s %-8s %-12s %-12s %-8s %-12s\n" "----" "----------" "----" "---------" "------" "----" "---------"
}

get_pod_metrics() {
    local pod_name="$1"

    # Get both metrics and limits in single kubectl calls
    local metrics_output=$(kubectl top pods "$pod_name" --no-headers 2>/dev/null)
    local limits_output=$(kubectl get pod "$pod_name" -o jsonpath='{.spec.containers[0].resources.limits.cpu} {.spec.containers[0].resources.limits.memory}' 2>/dev/null)

    # Parse metrics
    local cpu_usage=$(echo "$metrics_output" | awk '{print $2}')
    local mem_usage=$(echo "$metrics_output" | awk '{print $3}')

    # Parse limits
    local cpu_limit=$(echo "$limits_output" | awk '{print $1}')
    local mem_limit=$(echo "$limits_output" | awk '{print $2}')

    # Handle missing metrics
    [[ -z "$cpu_usage" ]] && cpu_usage="N/A"
    [[ -z "$mem_usage" ]] && mem_usage="N/A"
    [[ -z "$cpu_limit" ]] && cpu_limit="N/A"
    [[ -z "$mem_limit" ]] && mem_limit="N/A"

    # Calculate percentages
    local cpu_percent=$(calculate_percentage "$cpu_usage" "$cpu_limit")
    local mem_percent=$(calculate_percentage "$mem_usage" "$mem_limit")

    printf "%-30s %-12s %-8s %-12s %-12s %-8s %-12s\n" "$pod_name" "$cpu_usage" "$cpu_percent" "$cpu_limit" "$mem_usage" "$mem_percent" "$mem_limit"
}

# Function to monitor node resources
monitor_nodes() {
    echo "=== NODE METRICS ==="
    print_header

    # Get node metrics and capacity
    kubectl get nodes --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null | while read node_name; do
        if [[ -n "$node_name" ]]; then
            # Get current usage in single call
            local node_metrics=$(kubectl top nodes "$node_name" --no-headers 2>/dev/null)
            cpu_usage=$(echo "$node_metrics" | awk '{print $2}')
            mem_usage=$(echo "$node_metrics" | awk '{print $4}')

            # Get node capacity (limits) in single call
            local capacity_output=$(kubectl get node "$node_name" -o jsonpath='{.status.capacity.cpu} {.status.capacity.memory}' 2>/dev/null)
            cpu_capacity=$(echo "$capacity_output" | awk '{print $1}')
            mem_capacity=$(echo "$capacity_output" | awk '{print $2}')

            # If no metrics available, show N/A
            if [[ -z "$cpu_usage" ]]; then
                cpu_usage="N/A"
                mem_usage="N/A"
            fi

            # Handle missing capacity info
            [[ -z "$cpu_capacity" ]] && cpu_capacity="N/A"
            [[ -z "$mem_capacity" ]] && mem_capacity="N/A"

            # Convert CPU capacity to millicores for consistency
            if [[ -n "$cpu_capacity" && "$cpu_capacity" != "N/A" ]]; then
                cpu_capacity="${cpu_capacity}000m"
            fi

            # Convert memory capacity from Ki to Mi
            if [[ -n "$mem_capacity" && "$mem_capacity" != "N/A" && "$mem_capacity" =~ Ki$ ]]; then
                mem_num=$(echo "$mem_capacity" | sed 's/Ki//')
                mem_mi=$((mem_num / 1024))
                mem_capacity="${mem_mi}Mi"
            fi

            # Calculate percentages
            local cpu_percent=$(calculate_percentage "$cpu_usage" "$cpu_capacity")
            local mem_percent=$(calculate_percentage "$mem_usage" "$mem_capacity")

            printf "%-30s %-12s %-8s %-12s %-12s %-8s %-12s\n" "$node_name" "$cpu_usage" "$cpu_percent" "$cpu_capacity" "$mem_usage" "$mem_percent" "$mem_capacity"
        fi
    done
    echo
}

# Function to monitor pod resources
monitor_pods() {
    echo "=== POD METRICS ==="
    print_header

    # Process both app types using a loop - get all pods in single call
    kubectl get pods -l 'app in (tiled,postgres)' --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null | while read pod_name; do
        if [[ -n "$pod_name" ]]; then
            get_pod_metrics "$pod_name"
        fi
    done

    echo
    echo "=== POD STATUS ==="
    kubectl get pods -l app=tiled --no-headers -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp
    kubectl get pods -l app=postgres --no-headers -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp
    echo
}


# Main execution - single snapshot
monitor_nodes
monitor_pods