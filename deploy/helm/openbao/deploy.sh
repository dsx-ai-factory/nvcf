#!/bin/bash

# Usage
namespace="${1:-vault-system}"
statefulset="${2:-openbao-server}"

# Valid install methods
readonly INSTALL_METHOD_SCRIPT="script"
readonly INSTALL_METHOD_HELM="helm"

# Default to local if not specified
install_method="${3:-$INSTALL_METHOD_SCRIPT}"

# Validate install method
case "$install_method" in
"$INSTALL_METHOD_SCRIPT" | "$INSTALL_METHOD_HELM") ;;
*)
    log_error "Invalid install method: $install_method. Must be one of: $INSTALL_METHOD_SCRIPT, $INSTALL_METHOD_HELM"
    exit 1
    ;;
esac

DEPLOY_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -f "${DEPLOY_SCRIPT_DIR}/utils/utils.sh" ]; then
    echo "Error: utils.sh not found in ${DEPLOY_SCRIPT_DIR}/utils"
    exit 1
fi

source "$DEPLOY_SCRIPT_DIR/utils/utils.sh"
source "$DEPLOY_SCRIPT_DIR/helm/scripts/utils/jwt-plugin.sh"
source "$DEPLOY_SCRIPT_DIR/helm/scripts/utils/bootstrap.sh"

if ! check_kubernetes; then
    log_error "kubernetes check failed"
    exit 1
fi

# The chart has already installed OpenBao before this hook starts.
if [ "${install_method}" = "$INSTALL_METHOD_SCRIPT" ] && ! check_helm; then
    log_error "helm check failed"
    exit 1
fi

if [ "${install_method}" = "$INSTALL_METHOD_HELM" ]; then
    log_info "Checking jwker availability for helm deployment..."
    jwker_path="$(command -v jwker)"
    if [ -z "$jwker_path" ]; then
        log_error "jwker is not available in the init image. Use an nvcf-openbao-migrations image that includes jwker."
        exit 1
    fi
    log_success "jwker available at $jwker_path"
fi

# Helper function to get root token
get_root_token() {
    local namespace=$1 statefulset=$2 encoded
    encoded=$(bootstrap_kubectl get secret "$statefulset-root-token" -n "$namespace" \
        -o jsonpath='{.data.root_token}') || return 1
    printf '%s' "$encoded" | base64 -d
}

# Step 0: Pre-checks
pre_checks() {
    local namespace=$1
    local statefulset=$2

    log_section "Pre-checks"

    # Check if pod exists
    if ! kubectl get pod ${statefulset}-0 -n ${namespace} >/dev/null 2>&1; then
        log_info "Pod '${statefulset}-0' not found. Checking for existing PVCs..."
        # If pod doesn't exist, check for leftover PVCs
        # Capture the output and check exit code
        local PVC_OUTPUT=$(kubectl get pvc -l app.kubernetes.io/name=openbao -n ${namespace} 2>/dev/null)
        if [ $? -ne 0 ]; then
            log_error "Found existing PVCs but no pods. Please run ./cleanup.sh script first."
            return 1
        fi
    fi

    # If pod exists, check initialization status
    local init_status=$(kubectl exec ${statefulset}-0 -n ${namespace} -- \
        bao status -format=json | jq -r '.initialized')
    if [ "$init_status" = "true" ]; then
        log_error "OpenBao is already initialized. Please run ./cleanup.sh script first."
        return 1
    fi
}

# Step 1: Create empty secret
create_empty_secret() {
    local namespace=$1
    local statefulset=$2

    log_section "Setting up kubernetes secrets in namespace '${namespace}' and statefulset '${statefulset}'"
    log_info "Creating empty unseal secret..."
    kubectl create secret generic ${statefulset}-unseal \
        --from-literal=unseal_key="" \
        -n ${namespace}
}

# Step 2: Install OpenBao with Helm
install_openbao() {
    local namespace=$1
    local statefulset=$2

    log_section "Setting up OpenBao nodes"
    log_info "Installing OpenBao via Helm..."
    helm repo add openbao https://openbao.github.io/openbao-helm
    helm repo update openbao
    helm install -n ${namespace} ${statefulset} openbao/openbao --values helm/values.yaml --debug --set='global.imagePullSecrets[0].name=nvcr-secret'
}

# Step 3: Initialize the cluster
initialize_cluster() {
    local namespace=$1
    local statefulset=$2
    local bootstrap_deadline="" bootstrap_request_timeout=5

    log_section "Initializing OpenBao nodes"

    wait_for_openbao_api "$namespace" "$statefulset" || return 1
    check_bootstrap_credentials "$namespace" "$statefulset" shamir || return 1
    if [ "$BOOTSTRAP_INITIALIZED" = true ]; then
        return 0
    fi
    bootstrap_prepare_initialization "$namespace" || return 1
    log_info "Initializing OpenBao cluster"
    local init_output
    if ! init_output=$(bootstrap_kubectl exec ${statefulset}-0 -c openbao -n ${namespace} -- \
        env BAO_CLIENT_TIMEOUT="${OPENBAO_API_TIMEOUT_SECONDS-600}s" bao operator init -key-shares=1 -key-threshold=1 -format=json); then
        log_error "OpenBao initialization failed"
        return 1
    fi

    # Extract keys
    local unseal_key=$(echo ${init_output} | jq -r '.unseal_keys_b64[0]')
    local root_token=$(echo ${init_output} | jq -r '.root_token')

    # Check if unseal key is empty
    if [ -z "${unseal_key}" ] || [ "${unseal_key}" = null ] || [ -z "${root_token}" ] || [ "${root_token}" = null ]; then
        log_error "Failed to get unseal key from initialization output"
        return 1
    fi

    persist_bootstrap_credentials "$namespace" "$statefulset" unseal unseal_key \
        "$unseal_key" "$root_token" patch
}

get_unseal_key() {
    local namespace=$1 statefulset=$2 encoded
    encoded=$(bootstrap_kubectl get secret "$statefulset-unseal" -n "$namespace" \
        -o jsonpath='{.data.unseal_key}') || return 1
    printf '%s' "$encoded" | base64 -d
}

# Step 4: Unseal the cluster
unseal_cluster() {
    local namespace=$1 statefulset=$2
    unseal_openbao_members "$namespace" "$statefulset" shamir
}

get_and_save_jwt_signing_key() {
    local namespace=$1
    local statefulset=$2
    local jwt_pem_secret_name="cluster-jwt"

    log_section "Getting and saving Kubernetes JWT Signing key"

    log_info "Fetching JWT signing key from Kubernetes API"

    # Get the kubernetes service host ip from primary pod
    local kub_api_ip=$(kubectl exec ${statefulset}-0 -c openbao -n ${namespace} -- \
        printenv KUBERNETES_SERVICE_HOST)

    # get svc account token to access kubernetes api
    local svc_token=$(kubectl exec "${statefulset}-0" -c openbao -n "${namespace}" -- \
        cat /var/run/secrets/kubernetes.io/serviceaccount/token)

    # call kubernetes api to get the jwt signing key and decode it to a pem
    local jwt_pem=$(kubectl exec ${statefulset}-0 -c openbao -n ${namespace} -- \
        curl -s --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt --header "Authorization: Bearer ${svc_token}" \
        "https://${kub_api_ip}/openid/v1/jwks" | jq ".keys[0]" > /tmp/cluster.jwks && jwker /tmp/cluster.jwks | base64)

    # Check if jwt_pem is empty
    if [ -z "${jwt_pem}" ]; then
        log_error "Failed to get JWT signing key from Kubernetes API, or there was a problem with the jwker tool"
        return 1
    fi

    # write to file to avoid string errors when creating secret
    echo "${jwt_pem}" > /tmp/jwt.pem
    trap 'rm -f /tmp/jwt.pem /tmp/cluster.jwks' EXIT

    log_info "Creating/Updating JWT signing key in kubernetes secrets"

    # Check if secret exists first
    if ! kubectl get secret "${jwt_pem_secret_name}" -n ${namespace} >/dev/null 2>&1; then
        log_info "Saving JWT signing key to secret '${jwt_pem_secret_name}'"
        if ! kubectl create secret generic ${jwt_pem_secret_name} \
            -n ${namespace} \
            --from-file=pem=/tmp/jwt.pem; then
            log_error "Failed to create secret '${jwt_pem_secret_name}'"
            return 1
        fi
        log_success "JWT signing key saved to secret '${jwt_pem_secret_name}'"
        return 0
    else
        log_info "Kubernetes JWT Signing key already exists in secret '${jwt_pem_secret_name}', patching..."
        if ! kubectl patch secret "${jwt_pem_secret_name}" \
            -n ${namespace} \
            --patch "{\"data\":{\"pem\":\"${jwt_pem}\"}}"; then
            log_error "Failed to patch secret at '${jwt_pem_secret_name}'"
            return 1
        fi
        log_success "JWT signing key updated at '${jwt_pem_secret_name}'"
        return 0
    fi
}

log_section "Deploying OpenBao cluster in namespace '${namespace}' and statefulset '${statefulset}' using ${install_method} method"

if [ "${install_method}" = "script" ]; then
    if ! pre_checks ${namespace} ${statefulset}; then
        log_error "Failed pre-checks"
        exit 1
    else
        log_success "pre-checks completed"
    fi
else
    log_info "Skipping pre-checks as install_method is not 'script'"
fi

if [ "${install_method}" = "script" ]; then
    if ! create_empty_secret ${namespace} ${statefulset}; then
        exit 1
    else
        log_success "created empty unseal secret on k8s"
    fi
else
    log_info "Skipping local installation as install_method is not 'script'"
fi

if [ "${install_method}" = "script" ]; then
    if ! install_openbao ${namespace} ${statefulset}; then
        exit 1
    else
        log_success "Successfully deployed the cluster"
    fi
else
    log_info "Skipping local installation as install_method is not 'script'"
fi

if ! initialize_cluster ${namespace} ${statefulset}; then
    exit 1
else
    log_success "Successfully initialized the cluster"
fi

if ! unseal_cluster ${namespace} ${statefulset}; then
    exit 1
else
    log_success "Successfully unsealed the cluster"
fi

if ! get_and_save_jwt_signing_key ${namespace} ${statefulset}; then
    exit 1
else
    log_success "Successfully saved JWT signing key to kubernetes secrets"
fi

if ! register_and_enable_jwt_plugin ${namespace} ${statefulset}; then
    exit 1
else
    log_success "Successfully enabled JWT plugin"
fi

log_section "Install Successful"

log_success "Successfully deployed the OpenBao cluster to ${namespace} namespace"
