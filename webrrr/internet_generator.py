#!/usr/bin/env python3
import random
from http.server import BaseHTTPRequestHandler, HTTPServer

MAX_LINKS = 10
NUM_WEBSITES = 100
DOMAIN_BASE = "localhost"

PORT = 8000


def get_html(host: str, path: str) -> str:
    host_id = host.split(".")[0]

    random.seed(host_id + path)

    html = "<html>\n<body>\n"
    for _link_num in range(random.randint(0, MAX_LINKS)):
        host_id = random.randint(0, NUM_WEBSITES)
        link_id = random.randint(0, MAX_LINKS)

        url = f"http://{host_id}.{DOMAIN_BASE}:{PORT}/{link_id}"
        html += f'<a href="{url}">{url}</a>\n<br>\n'
    html += "</body></html>"

    return html


class FakeInternetHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        host = self.headers.get("HOST")

        html = get_html(host, self.path).encode()

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", f"{len(html)}")
        self.end_headers()


        self.wfile.write(html)


def run(server_class=HTTPServer, handler_class=FakeInternetHTTPRequestHandler) -> None:
    server_address = ("", PORT)
    httpd = server_class(server_address, handler_class)

    httpd.serve_forever()


if __name__ == "__main__":
    run()
