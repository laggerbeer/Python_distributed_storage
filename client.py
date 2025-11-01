# client.py
import socket
import json
import urllib.parse

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 8080

def send_http_request(method, path, body=None, content_type="application/octet-stream"):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            headers = [
                f"{method} {path} HTTP/1.1",
                f"Host: {MASTER_HOST}",
                "Connection: close"
            ]
            if body is not None:
                headers.append(f"Content-Length: {len(body)}")
                headers.append(f"Content-Type: {content_type}")
            request = "\r\n".join(headers) + "\r\n\r\n"
            s.sendall(request.encode('latin1') + (body or b""))

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
        print(f"Ошибка: {e}")
        return 500, b""

def main():
    print("Клиент")
    print("Команды: node create <id> <имя>, node list, node delete <id>")
    print("         put <ключ> <значение>, get <ключ>, get all, delete <ключ>, quit\n")

    while True:
        try:
            inp = input(">>> ").strip()
            if not inp:
                continue
            parts = inp.split()
            cmd = parts[0].lower()

            if cmd == "quit":
                break

            elif cmd == "node":
                if len(parts) < 2:
                    print("node [create|list|delete] ...")
                    continue
                sub = parts[1].lower()
                if sub == "create":
                    if len(parts) < 4:
                        print("node create <id> <имя>")
                        continue
                    try:
                        node_id = int(parts[2])
                        name = parts[3]
                    except:
                        print("ID должен быть числом")
                        continue
                    body = json.dumps({"name": name}).encode('utf-8')
                    code, _ = send_http_request("PUT", f"/node/{node_id}", body, "application/json")
                    if code == 201:
                        print(f"Нода {node_id} ('{name}') создана. Запустите: worker.py {8000+node_id} {name} {node_id}")
                    else:
                        print(f"Ошибка: {code}")

                elif sub == "list":
                    code, body = send_http_request("GET", "/node")
                    if code == 200:
                        try:
                            names = json.loads(body.decode('utf-8'))
                            for i, n in enumerate(names):
                                print(f"  {i+1}. {n}")
                        except:
                            print("Ошибка разбора")
                    else:
                        print(f"Ошибка: {code}")

                elif sub == "delete":
                    if len(parts) < 3:
                        print("node delete <id>")
                        continue
                    try:
                        node_id = int(parts[2])
                    except:
                        print("ID — число")
                        continue
                    code, _ = send_http_request("DELETE", f"/node/{node_id}")
                    if code == 200:
                        print(f"Нода {node_id} удалена")
                    else:
                        print(f"Ошибка: {code}")
                else:
                    print("Неизвестная команда")

            elif cmd == "put":
                if len(parts) < 3:
                    print("put <ключ> <значение>")
                    continue
                key, value = parts[1], parts[2]
                path = f"/storage/{urllib.parse.quote(key, safe='')}"
                code, _ = send_http_request("PUT", path, value.encode('utf-8'))
                print("Удачно" if code == 201 else f"ERROR {code}")

            elif cmd == "get":
                if len(parts) < 2:
                    print("get <ключ> или get all")
                    continue
                if parts[1] == "all":
                    code, body = send_http_request("GET", "/storage")
                    if code == 200:
                        try:
                            keys = json.loads(body.decode('utf-8'))
                            print("Ключи:", keys)
                        except:
                            print("Ошибка разбора")
                    else:
                        print(f"{code}")
                else:
                    key = parts[1]
                    path = f"/storage/{urllib.parse.quote(key, safe='')}"
                    code, body = send_http_request("GET", path)
                    if code == 200:
                        print("Значение:", body.decode('utf-8', errors='replace'))
                    else:
                        print(f"{code}")

            elif cmd == "delete":
                if len(parts) < 2:
                    print("delete <ключ>")
                    continue
                key = parts[1]
                path = f"/storage/{urllib.parse.quote(key, safe='')}"
                code, _ = send_http_request("DELETE", path)
                print("Удалено" if code == 200 else f"{code}")

            else:
                print("Неизвестная команда")

        except KeyboardInterrupt:
            break

    print("Выход")

if __name__ == "__main__":
    main()