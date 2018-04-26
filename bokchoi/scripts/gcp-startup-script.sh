#!/bin/bash

apt-get install unzip

BUCKET_NAME=$(curl http://metadata/computeMetadata/v1/instance/attributes/bucket_name -H "Metadata-Flavor: Google")
PACKAGE_NAME=$(curl http://metadata/computeMetadata/v1/instance/attributes/package_name -H "Metadata-Flavor: Google")
ENTRYPOINT=$(curl http://metadata/computeMetadata/v1/instance/attributes/entry_point -H "Metadata-Flavor: Google")

gsutil cp gs://${BUCKET_NAME}/${PACKAGE_NAME} .
unzip ${PACKAGE_NAME}
python ${ENTRYPOINT}