OPAQUE_HOME=$HOME/opaque

source /opt/intel/sgxsdk/environment # from SGX SDK install directory in step 1
export CXX=/usr/bin/g++-4.8
export SPARKSGX_DATA_DIR=${OPAQUE_HOME}/data
export LIBSGXENCLAVE_PATH=${OPAQUE_HOME}/libSGXEnclave.so
export LIBENCLAVESIGNED_PATH=${OPAQUE_HOME}/enclave.signed.so
export LIBSGX_SP_PATH=${OPAQUE_HOME}/libservice_provider.so
export PRIVATE_KEY_PATH=${OPAQUE_HOME}/private_key.pem