FROM python:3.11-slim

WORKDIR /app
COPY scripts/mock_ui.py scripts/mock_ui.py
COPY scripts/mock_ui_assets scripts/mock_ui_assets

RUN pip install --no-cache-dir aiohttp websockets

ENV MOCK_UI_PORT=5001 \
    MOCK_UI_PUBLIC_HOST=mock-ui \
    CONNECTIVITY_UI_ASSET_PATHS=/app/scripts/mock_ui_assets

EXPOSE 5001
CMD ["python", "scripts/mock_ui.py"]
