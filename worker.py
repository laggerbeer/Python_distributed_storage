# worker.py
import socket
import threading
import os
import json
import sys
import signal
import urllib.parse
import re
from datetime import datetime
import time

if len(sys.argv) != 4:
    print("Запуск: python worker.py <порт> <имя_ноды> <id_ноды>")
    sys.exit(1)

PORT = int(sys.argv[1])
NODE_NAME = sys.argv[2]
NODE_ID = int(sys.argv[3])
HOST = "127.0.0.1"
MASTER_HOST, MASTER_PORT = "127.0.0.1", 8080
DATA_DIR = f"basic_node/worker_data_{PORT}"
os.makedirs(DATA_DIR, exist_ok=True)

def register_with_master():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            body = json.dumps({"id": NODE_ID, "port": PORT}).encode('utf-8')
            request = (
                f"POST /register HTTP/1.1\r\n"
                f"Host: {MASTER_HOST}\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Content-Type: application/json\r\n"
                "Connection: close\r\n\r\n"
            ).encode('latin1') + body
            s.sendall(request)
            response = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
            print(f"Зарегистрирован на мастере как нода {NODE_ID}")
    except Exception as e:
        print(f"Ошибка регистрации: {e}")

def safe_filename(key):
    safe = ""
    for c in key:
        if c.isalnum() or c in "._-":
            safe += c
        else:
            safe += "_"
    return safe or "_"

def list_keys():
    return [f for f in os.listdir(DATA_DIR) if os.path.isfile(os.path.join(DATA_DIR, f))]

def handle_client(conn, addr):
    try:
        data = conn.recv(1024 * 1024)
        if not data:
            return

        request_str = data.decode('latin1', errors='ignore')
        lines = request_str.split('\r\n')
        if not lines:
            send_response(conn, 400, b"Bad Request")
            return

        request_line = lines[0]
        match = re.match(r'(GET|PUT|DELETE)\s+(.+?)\s+HTTP/1\.1', request_line)
        if not match:
            send_response(conn, 400, b"Invalid HTTP request")
            return

        method, raw_path = match.groups()
        path = urllib.parse.unquote(raw_path)

        if not (path == "/storage" or path.startswith("/storage/")):
            send_response(conn, 400, b"Use /storage or /storage/<key>")
            return

        key = path[len('/storage/'):] if path.startswith('/storage/') else ""
        print(f"[{NODE_NAME}] Получен запрос: {method} /storage/{repr(key)}")

        if method == 'PUT':
            body_start = data.find(b'\r\n\r\n') + 4
            body = data[body_start:] if body_start > 3 else b""
            if not body:
                send_response(conn, 400, b"Body required")
                return
            file_path = os.path.join(DATA_DIR, safe_filename(key))
            with open(file_path, 'wb') as f:
                f.write(body)
            print(f"[{NODE_NAME}] Ответ: 201 Created")
            send_response(conn, 201, b"OK")

        elif method == 'GET':
            if key == "":
                keys = list_keys()
                response_body = json.dumps(keys, ensure_ascii=False).encode('utf-8')
                print(f"[{NODE_NAME}] Ответ: 200 (все ключи)")
                send_response(conn, 200, response_body, "application/json")
            else:
                file_path = os.path.join(DATA_DIR, safe_filename(key))
                if not os.path.exists(file_path):
                    print(f"[{NODE_NAME}] Ответ: 404 Not Found")
                    send_response(conn, 404, b"Key not found")
                    return
                with open(file_path, 'rb') as f:
                    content = f.read()
                print(f"[{NODE_NAME}] Ответ: 200 OK (длина: {len(content)} байт)")
                send_response(conn, 200, content, "application/octet-stream")

        elif method == 'DELETE':
            file_path = os.path.join(DATA_DIR, safe_filename(key))
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[{NODE_NAME}] Ответ: 200 Deleted")
                send_response(conn, 200, b"Deleted")
            else:
                print(f"[{NODE_NAME}] Ответ: 404 Not Found")
                send_response(conn, 404, b"Key not found")

    except Exception as e:
        print(f"[{NODE_NAME} ERROR] {e}")
        send_response(conn, 500, b"Internal Error")
    finally:
        conn.close()

def send_response(conn, status_code, body, content_type="text/plain"):
    reason = {200: "OK", 201: "Created", 400: "Bad Request", 404: "Not Found", 500: "Internal Error"}.get(status_code, "Unknown")
    response = (
        f"HTTP/1.1 {status_code} {reason}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        f"Date: {datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n"
        "\r\n"
    ).encode('latin1') + body
    conn.sendall(response)

def signal_handler(sig, frame):
    print("Завершение работы...")
    sys.exit(0)

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((HOST, PORT))
    sock.listen(5)
    print(f"Рабочая нода '{NODE_NAME}' (ID={NODE_ID}) запущена на порту {PORT}")

    time.sleep(0.2)
    register_with_master()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()

if __name__ == "__main__":
    main()