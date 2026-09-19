"""
Turns a parsed node dict into a minimal Xray JSON config: one SOCKS inbound
on 127.0.0.1:<local_port>, one outbound pointing at the remote node.

When dialer is set (IR track), the node outbound is chained through that
SOCKS/HTTP hop via sockopt.dialerProxy so the handshake egresses from
Iran, not from the GitHub runner.
"""


def _normalize_network(network: str) -> str:
    aliases = {
        "websocket": "ws",
        "splithttp": "xhttp",
        "http": "h2",
        "raw": "tcp",
    }
    return aliases.get((network or "tcp").lower(), (network or "tcp").lower())


def _tls_settings(node: dict) -> dict:
    settings = {
        "serverName": node["sni"],
        "allowInsecure": bool(node.get("allow_insecure", False)),
        "fingerprint": node.get("fp") or "chrome",
    }
    alpn = node.get("alpn") or []
    if alpn:
        settings["alpn"] = alpn
    return settings


def _reality_settings(node: dict) -> dict:
    settings = {
        "serverName": node["sni"],
        "publicKey": node.get("pbk", ""),
        "shortId": node.get("sid", ""),
        "fingerprint": node.get("fp") or "chrome",
    }
    if node.get("spx"):
        settings["spiderX"] = node["spx"]
    return settings


def _apply_transport(stream: dict, node: dict) -> None:
    network = _normalize_network(node.get("network", "tcp"))
    host = node.get("host_header") or node["address"]
    path = node.get("path") or "/"
    header_type = (node.get("header_type") or "none").lower()
    mode = node.get("mode") or ""

    # Dedicated h2 transport was removed in current Xray; xhttp covers HTTP/2.
    if network == "h2":
        network = "xhttp"
        if not mode:
            mode = "auto"

    stream["network"] = network

    if network == "ws":
        stream["wsSettings"] = {
            "path": path,
            "headers": {"Host": host},
        }
    elif network == "grpc":
        grpc = {"serviceName": node.get("service_name", "")}
        if mode.lower() == "multi":
            grpc["multiMode"] = True
        if node.get("authority"):
            grpc["authority"] = node["authority"]
        stream["grpcSettings"] = grpc
    elif network == "httpupgrade":
        stream["httpupgradeSettings"] = {
            "path": path,
            "host": host,
        }
    elif network == "xhttp":
        stream["xhttpSettings"] = {
            "path": path,
            "host": host,
            "mode": mode or "auto",
        }
    elif network == "tcp" and header_type == "http":
        stream["tcpSettings"] = {
            "header": {
                "type": "http",
                "request": {
                    "path": [path or "/"],
                    "headers": {"Host": [host]},
                },
            }
        }


def _dialer_outbound(dialer: dict) -> dict:
    protocol = (dialer.get("protocol") or "socks").lower()
    if protocol == "https":
        protocol = "http"
    if protocol not in ("socks", "http"):
        protocol = "socks"
    server = {
        "address": dialer["address"],
        "port": int(dialer["port"]),
    }
    if dialer.get("username"):
        server["users"] = [{
            "user": dialer["username"],
            "pass": dialer.get("password") or "",
        }]
    return {
        "tag": "ir",
        "protocol": protocol,
        "settings": {"servers": [server]},
    }


def build_config(node: dict, local_port: int, dialer: dict | None = None) -> dict:
    stream_settings: dict = {}
    _apply_transport(stream_settings, node)

    security = (node.get("security") or "none").lower()
    if security == "tls":
        stream_settings["security"] = "tls"
        stream_settings["tlsSettings"] = _tls_settings(node)
    elif security == "reality":
        stream_settings["security"] = "reality"
        stream_settings["realitySettings"] = _reality_settings(node)

    if dialer:
        stream_settings.setdefault("sockopt", {})
        stream_settings["sockopt"]["dialerProxy"] = "ir"

    flow = node.get("flow") or ""
    network = stream_settings.get("network", "tcp")
    if flow and not (network == "tcp" and security in ("tls", "reality")):
        flow = ""

    if node["type"] == "vless":
        outbound = {
            "protocol": "vless",
            "tag": "node",
            "settings": {
                "vnext": [{
                    "address": node["address"],
                    "port": node["port"],
                    "users": [{
                        "id": node["uuid"],
                        "encryption": node.get("encryption", "none"),
                        "flow": flow,
                    }],
                }]
            },
            "streamSettings": stream_settings,
        }
    else:
        outbound = {
            "protocol": "trojan",
            "tag": "node",
            "settings": {
                "servers": [{
                    "address": node["address"],
                    "port": node["port"],
                    "password": node["password"],
                }]
            },
            "streamSettings": stream_settings,
        }

    outbounds = [outbound]
    if dialer:
        outbounds.append(_dialer_outbound(dialer))

    return {
        "log": {"loglevel": "none"},
        "inbounds": [{
            "listen": "127.0.0.1",
            "port": local_port,
            "protocol": "socks",
            "settings": {"auth": "noauth", "udp": False},
        }],
        "outbounds": outbounds,
    }
