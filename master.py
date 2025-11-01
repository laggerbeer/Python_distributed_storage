import socket
import threading
import json
import time
import random
import urllib.parse
import re
from datetime import datetime
import hashlib

workers = {}
workers_lock = threading.Lock()

def get_active_workers():
    """Возвращает список активных нод (с портом != 0)"""
    with workers_lock:
        return [w for w in workers.values() if w["port"] != 0]

def get_target_node(key):
    """Определяет целевую ноду для ключа по алгоритму шардирования."""
    active_workers = get_active_workers()
    if not active_workers:
        return None

    # Простой хеш для равномерного распределения
    # Используем SHA256 и конвертируем в число
    key_hash = int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16)
    target_index = key_hash % len(active_workers)
    return active_workers[target_index]

def send_http_request(host, port, method, path, body=b"", timeout=10):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            headers = [
                f"{method} {path} HTTP/1.1",
                f"Host: {host}",
                "Connection: close"
            ]
            if body:
                headers.append(f"Content-Length: {len(body)}")
                headers.append("Content-Type: application/octet-stream")
            request = "\r\n".join(headers) + "\r\n\r\n"
            s.sendall(request.encode('latin1') + body)

            response = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk

        header_end = response.find(b"\r\n\r\n")
        if header_end == -1:
            return 500, b"Invalid response"
        status_line = response[:header_end].split(b'\r\n')[0]
        status_code = int(status_line.split(b' ')[1])
        body = response[header_end + 4:]
        return status_code, body
    except Exception as e:
        print(f"[MASTER] Ошибка при обращении к ноде ({host}:{port}): {e}")
        return 500, b""

# Синхронизация больше не требуется в таком виде, так как данные не копируются между нодами.
# def sync_new_node(node_info):
#     ...

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
        match = re.match(r'(GET|PUT|DELETE|POST)\s+(.+?)\s+HTTP/1\.1', request_line)
        if not match:
            send_response(conn, 400, b"Invalid HTTP request")
            return

        method, raw_path = match.groups()
        path = urllib.parse.unquote(raw_path)

        # --- Регистрация ноды ---
        if path == "/register" and method == "POST":
            body_start = data.find(b'\r\n\r\n') + 4
            try:
                body = data[body_start:]
                payload = json.loads(body.decode('utf-8'))
                node_id = payload.get("id")
                port = payload.get("port")
                if not isinstance(node_id, int) or not isinstance(port, int):
                    send_response(conn, 400, b"Invalid id or port")
                    return
                with workers_lock:
                    if node_id in workers:
                        workers[node_id]["port"] = port
                        print(f"[MASTER] Нода {node_id} зарегистрирована на порту {port}")
                        # Синхронизация больше не нужна для шардинга
                        # threading.Thread(target=sync_new_node, args=(workers[node_id],), daemon=True).start()
                    else:
                        print(f"[MASTER] Нода {node_id} не создана через PUT /node/{node_id}")
                        send_response(conn, 404, b"Node not pre-registered")
                        return
                send_response(conn, 200, b"OK")
                return
            except Exception as e:
                print(f"[MASTER] Ошибка регистрации: {e}")
                send_response(conn, 400, b"Bad Request")
                return

        # --- Управление нодами ---
        if path == "/node" and method == "GET":
            with workers_lock:
                names = [w["name"] for w in workers.values()]
            response_body = json.dumps(names, ensure_ascii=False).encode('utf-8')
            send_response(conn, 200, response_body, "application/json")
            return

        if path.startswith('/node/') and len(path.split('/')) == 3:
            node_id_str = path.split('/')[2]
            if method == 'PUT' and node_id_str.isdigit():
                handle_create_node(conn, int(node_id_str), data)
                return
            elif method == 'DELETE' and node_id_str.isdigit():
                handle_delete_node(conn, int(node_id_str))
                return

        # --- Хранилище (ШАРДИРОВАНИЕ) ---
        if not (path == "/storage" or path.startswith("/storage/")):
            send_response(conn, 400, b"Use /storage or /storage/<key>")
            return

        # Для /storage (получение всех ключей) пока оставим выбор первой ноды
        # или можно реализовать объединение ключей со всех нод, но это дорого.
        # Пусть будет с первой активной.
        key = path[len('/storage/'):] if path.startswith('/storage/') else ""

        if key == "":
            # GET /storage - получить все ключи (с одной ноды)
            active_workers = get_active_workers()
            if not active_workers:
                send_response(conn, 503, b"No workers available")
                return
            w = active_workers[0]
            code, body = send_http_request(w['host'], w['port'], 'GET', "/storage")
            if code == 200:
                send_response(conn, 200, body, "application/json")
            else:
                send_response(conn, 500, b"Failed to fetch keys")
            return

        # Для конкретного ключа используем шардирование
        target_node_info = get_target_node(key)
        if not target_node_info:
            send_response(conn, 503, b"No workers available")
            return

        host, port = target_node_info['host'], target_node_info['port']
        encoded_key = urllib.parse.quote(key, safe='')

        if method == 'PUT':
            body_start = data.find(b'\r\n\r\n') + 4
            body = data[body_start:] if body_start > 3 else b""
            code, _ = send_http_request(host, port, 'PUT', f"/storage/{encoded_key}", body)
            if code == 201:
                send_response(conn, 201, b"OK")
            else:
                send_response(conn, 500, b"Failed to store key")

        elif method == 'GET':
            code, body = send_http_request(host, port, 'GET', f"/storage/{encoded_key}")
            if code == 200:
                send_response(conn, 200, body, "application/octet-stream")
            elif code == 404:
                send_response(conn, 404, b"Key not found")
            else:
                send_response(conn, 500, b"Failed to retrieve key")

        elif method == 'DELETE':
            code, _ = send_http_request(host, port, 'DELETE', f"/storage/{encoded_key}")
            if code == 200:
                send_response(conn, 200, b"Deleted")
            elif code == 404:
                send_response(conn, 404, b"Key not found")
            else:
                send_response(conn, 500, b"Failed to delete key")

    except Exception as e:
        print(f"[MASTER ERROR] {e}")
        send_response(conn, 500, b"Internal Error")
    finally:
        conn.close()

def handle_create_node(conn, node_id, raw_request):
    body_start = raw_request.find(b'\r\n\r\n') + 4
    try:
        body = raw_request[body_start:]
        payload = json.loads(body.decode('utf-8'))
        name = payload.get("name")
        if not name or not isinstance(name, str):
            send_response(conn, 400, b"Missing or invalid 'name'")
            return
    except:
        send_response(conn, 400, b"Invalid JSON")
        return

    with workers_lock:
        workers[node_id] = {
            "name": name,
            "host": "127.0.0.1",
            "port": 0, # Пока не зарегистрирована
            "id": node_id
        }
    send_response(conn, 201, b"", "text/plain")
    print(f"[MASTER] Создана запись для ноды {node_id} ('{name}')")

def handle_delete_node(conn, node_id):
    with workers_lock:
        if node_id in workers:
            # При удалении ноды она становится "неактивной" (port = 0)
            # Это приведёт к ребалансировке при следующем запросе
            workers[node_id]["port"] = 0
            # Данные на ней теряются (или остаются, но становятся недоступны через мастер)
            send_response(conn, 200, b"Node marked as inactive (data potentially lost)")
        else:
            send_response(conn, 404, b"Node not found")

def send_response(conn, status_code, body, content_type="text/plain"):
    reason = {200: "OK", 201: "Created", 400: "Bad Request", 404: "Not Found", 500: "Internal Error", 503: "Service Unavailable"}.get(status_code, "Unknown")
    response = (
        f"HTTP/1.1 {status_code} {reason}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        f"Date: {datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n"
        "\r\n"
    ).encode('latin1') + body
    conn.sendall(response)

def main():
    HOST, PORT = "127.0.0.1", 8080
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((HOST, PORT))
    sock.listen(10)
    print(f"Мастер-нода (Шардирование) запущена на http://{HOST}:{PORT}")

    try:
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("\nМастер остановлен.")
    finally:
        sock.close()

if __name__ == "__main__":
    main()