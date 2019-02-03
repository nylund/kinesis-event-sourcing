#!/bin/bash

URL=--endpoint-url=http://localhost:4568
aws $URL kinesis create-stream --stream-name test-input-stream-1 --shard-count 2

aws $URL kinesis list-streams