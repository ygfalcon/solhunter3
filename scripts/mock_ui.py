import http.server
import socketserver
import json
import os

PORT = int(os.getenv("MOCK_UI_PORT", "8002"))


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/healthz"):
            payload = json.dumps({"ok": True, "component": "mock_ui"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"mock ui")


if __name__ == "__main__":
    with socketserver.TCPServer(("0.0.0.0", PORT), Handler) as httpd:
        httpd.serve_forever()
