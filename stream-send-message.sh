#!/bin/bash

URL=--endpoint-url=http://localhost:4568
STREAM=test-input-stream-1
DATA=`cat stream-message-1.json`

echo "Sending '$DATA'"
aws $URL kinesis put-record --stream-name $STREAM --partition-key 1234 --data "$DATA"
