#!/bin/bash

uvicorn api.app:app \
    --host 0.0.0.0 \
    --port 10000 \
    --workers 1 \
    --timeout-keep-alive 30 \