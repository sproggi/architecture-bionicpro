import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CONTACTS = [
    {"ID": "u-101", "LAST_NAME": "Ivanov", "NAME": "Ivan", "EMAIL": "u-101@example.com"},
    {"ID": "u-102", "LAST_NAME": "Petrova", "NAME": "Anna", "EMAIL": "u-102@example.com"},
    {"ID": "u-103", "LAST_NAME": "Sidorov", "NAME": "Pavel", "EMAIL": "u-103@example.com"},
]

DEALS = [
    {
        "ID": "d-001",
        "C_TITLE": "Протез BionicPRO X1",
        "CONTACT_IDS": ["u-101"],
        "PRODUCT": "BionicPRO X1",
        "PROSTHESIS_ID": "p-201",
        "DATE_CLOSED": "2026-09-15T12:00:00+03:00",
    },
    {
        "ID": "d-002",
        "C_TITLE": "Протез BionicPRO X1",
        "CONTACT_IDS": ["u-102"],
        "PRODUCT": "BionicPRO X1",
        "PROSTHESIS_ID": "p-202",
        "DATE_CLOSED": "2026-10-02T10:30:00+03:00",
    },
    {
        "ID": "d-003",
        "C_TITLE": "Протез BionicPRO X2",
        "CONTACT_IDS": ["u-103"],
        "PRODUCT": "BionicPRO X2",
        "PROSTHESIS_ID": "p-203",
        "DATE_CLOSED": "2026-10-20T16:45:00+03:00",
    },
]


class CrmHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)

        if self.path.startswith("/rest/crm.deal.list"):
            result = DEALS
        elif self.path.startswith("/rest/crm.contact.list"):
            result = CONTACTS
        else:
            self.send_response(404)
            self.end_headers()
            return

        body = json.dumps(
            {"result": result, "result_total": len(result), "result_next": None}
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), CrmHandler).serve_forever()
