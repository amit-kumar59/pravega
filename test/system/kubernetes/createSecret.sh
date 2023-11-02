#!/usr/bin/env bash
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Set the namespace, we want to create the secret
NAMESPACE="default"
#tlsEnabled=true
#securityEnabled=true

# Define the secrets we want to delete
secrets_to_delete=("password-auth" "controller-tls" "segmentstore-tls")

# Delete secret if it exists
if [ "$tlsEnabled" == "true" ] && [ "$securityEnabled" == "true" ]; then
    # Delete existing secrets
    #kubectl delete secret password-auth controller-tls segmentstore-tls
    # Loop through and delete each secret if it exists
    for secret in "${secrets_to_delete[@]}"; do
        kubectl get secret $secret &> /dev/null
        if [ $? -eq 0 ]; then
            echo "Deleting secret $secret"
            kubectl delete secret $secret
        else
            echo "Secret $secret does not exist, skipping deletion"
        fi
    done
fi

# Set the secret name
CONTROLLER_SECRET_NAME="controller-tls"
SEGMENT_STORE_SECRET_NAME="segmentstore-tls"
PASSWORD_AUTH_SECRET_NAME="password-auth"

PATH=$(pwd)
cd ../
CERTIFICATE_PATH=$(pwd)/src/test/resources
echo "Certificates path : $CERTIFICATE_PATH"

# Set the paths to the files, which we want to include in the secret
#For Controller
CONTROLLER_PEM_FILE="$CERTIFICATE_PATH/controller01.pem"
CONTROLLER_KEY_PEM="$CERTIFICATE_PATH/controller01.key.pem"
CONTROLLER_JKS_FILE="$CERTIFICATE_PATH/controller01.jks"

#For Segment store
SEGMENT_STORE_PEM_FILE="$CERTIFICATE_PATH/segmentstore01.pem"
SEGMENT_STORE_KEY_PEM="$CERTIFICATE_PATH/segmentstore01.key.pem"
SEGMENT_STORE_JKS_FILE="$CERTIFICATE_PATH/segmentstore01.jks"

TLS_CRT="$CERTIFICATE_PATH/tls.crt"
PASS_SECRET="$CERTIFICATE_PATH/pass-secret-tls"
PASS_SECRET_TLS_AUTH="$CERTIFICATE_PATH/pass-secret-tls-auth.txt"

# Create the password-auth secret
kubectl create secret generic $PASSWORD_AUTH_SECRET_NAME --from-file=$PASS_SECRET_TLS_AUTH --namespace=$NAMESPACE

# Checking if the password-auth secret creation was successful
if [ $? -eq 0 ]; then
  echo "Kubernetes secret '$PASSWORD_AUTH_SECRET_NAME' created successfully in namespace '$NAMESPACE'."
else
  echo "Error creating Kubernetes secret '$PASSWORD_AUTH_SECRET_NAME' in namespace '$NAMESPACE'."
fi

# Create the controller-tls secret
kubectl create secret generic $CONTROLLER_SECRET_NAME \
  --namespace=$NAMESPACE \
  --from-file=$CONTROLLER_PEM_FILE \
  --from-file=$CONTROLLER_TLS_CRT \
  --from-file=$CONTROLLER_KEY_PEM \
  --from-file=$CONTROLLER_JKS_FILE \
  --from-file=$CONTROLLER_PASS_SECRET

# Checking if the controller-tls secret creation was successful
if [ $? -eq 0 ]; then
  echo "Kubernetes secret '$CONTROLLER_SECRET_NAME' created successfully in namespace '$NAMESPACE'."
else
  echo "Error creating Kubernetes secret '$CONTROLLER_SECRET_NAME' in namespace '$NAMESPACE'."
fi

# Create the segment store-tls secret
kubectl create secret generic $SEGMENT_STORE_SECRET_NAME \
  --namespace=$NAMESPACE \
  --from-file=$SEGMENT_STORE_PEM_FILE \
  --from-file=$TLS_CRT \
  --from-file=$SEGMENT_STORE_KEY_PEM \
  --from-file=$SEGMENT_STORE_JKS_FILE \
  --from-file=$PASS_SECRET

# Checking if the segment store-tls secret creation was successful
if [ $? -eq 0 ]; then
  echo "Kubernetes secret '$SEGMENT_STORE_SECRET_NAME' created successfully in namespace '$NAMESPACE'."
else
  echo "Error creating Kubernetes secret '$SEGMENT_STORE_SECRET_NAME' in namespace '$NAMESPACE'."
fi