import io
import json
import logManager
import requests
import threading
import time

from functions.colors import convert_xy

try:
    from xled import HighControlInterface
except Exception:  # pragma: no cover
    HighControlInterface = None

logging = logManager.logger.get_logger(__name__)

_buffers = {}
_buffers_lock = threading.Lock()


def _coerce_rgb(data, light):
    rgb = None
    bri = int(data.get("bri", light.state.get("bri", 255))) if hasattr(light, "state") else int(data.get("bri", 255))

    if "xy" in data:
        x, y = data["xy"]
        rgb = convert_xy(x, y, bri)
    elif "ct" in data:
        # Approximate ct->rgb fallback; keeps the Hue API usable without a separate ct converter.
        ct = max(153, min(500, int(data["ct"])))
        if ct <= 153:
            rgb = [255, 175, 72]
        elif ct >= 500:
            rgb = [255, 255, 255]
        else:
            ratio = (ct - 153) / (500 - 153)
            r = int(255 * (0.8 + ratio * 0.2))
            g = int(180 + ratio * 60)
            b = int(120 + ratio * 120)
            rgb = [max(0, min(255, r)), max(0, min(255, g)), max(0, min(255, b))]
    elif "rgb" in data:
        rgb = list(data["rgb"])
    elif all(k in data for k in ("r", "g", "b")):
        rgb = [int(data["r"]), int(data["g"]), int(data["b"])]
    elif "hue" in data and "sat" in data:
        rgb = _hsv_to_rgb(data["hue"], data["sat"], bri)
    else:
        rgb = [255, 255, 255]

    if bri is not None and bri < 255:
        rgb = [int(v * bri / 255.0) for v in rgb]
    return [max(0, min(255, int(v))) for v in rgb]


def _hsv_to_rgb(hue, sat, bri):
    r = 0
    g = 0
    b = 0
    h = max(0, min(65535, int(hue)))
    s = max(0, min(254, int(sat)))
    v = max(0, min(254, int(bri)))
    h = (h / 65535.0) * 360.0
    s = s / 254.0
    v = v / 254.0
    c = v * s
    x = c * (1 - abs((h / 60.0) % 2 - 1))
    m = v - c
    if 0 <= h < 60:
        r, g, b = c, x, 0
    elif 60 <= h < 120:
        r, g, b = x, c, 0
    elif 120 <= h < 180:
        r, g, b = 0, c, x
    elif 180 <= h < 240:
        r, g, b = 0, x, c
    elif 240 <= h < 300:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x
    return [int((r + m) * 255), int((g + m) * 255), int((b + m) * 255)]


def _get_device_state(ip):
    response = requests.get(f"http://{ip}/xled/v1/gestalt", timeout=3)
    if response.status_code != 200:
        raise RuntimeError(f"Twinkly gestalt call failed for {ip}: {response.status_code}")
    payload = response.json()
    return payload


def _get_total_leds(info):
    led_count = info.get("leds")
    if isinstance(led_count, dict):
        led_count = led_count.get("count")
    if isinstance(led_count, int):
        return led_count
    if isinstance(info.get("led_count"), int):
        return info["led_count"]
    if isinstance(info.get("number_of_led"), int):
        return info["number_of_led"]
    return 150


def _get_name(info, fallback):
    return info.get("name") or info.get("device_name") or fallback or "Twinkly"


def _segment_range_for_index(led_total, segment_index, segment_count):
    if segment_count <= 1:
        return 0, led_total - 1
    segment_size = max(1, led_total // segment_count)
    start = segment_index * segment_size
    end = (segment_index + 1) * segment_size - 1 if segment_index < segment_count - 1 else led_total - 1
    return start, end


def _ensure_device(host, led_total):
    if HighControlInterface is None:
        raise RuntimeError("xled is not installed. Install it with: pip install xled")
    device_host = host.rsplit(":", 1)[0] if host.count(":") == 1 else host
    key = (host, int(led_total))
    with _buffers_lock:
        bridge = _buffers.get(key)
        if bridge is None:
            bridge = {
                "device": HighControlInterface(device_host),
                "pixels": bytearray(led_total * 3),
                "led_total": led_total,
                "fps": 30,
                "last_flush": 0.0,
                "lock": threading.Lock(),
            }
            try:
                bridge["device"].set_mode("rt")
            except Exception:
                pass
            _buffers[key] = bridge
    return _buffers[key]


def _paint_segment(host, led_total, start_idx, end_idx, rgb):
    bridge = _ensure_device(host, led_total)
    with bridge["lock"]:
        r, g, b = rgb
        for idx in range(max(0, start_idx), min(led_total - 1, end_idx) + 1):
            offset = idx * 3
            bridge["pixels"][offset] = int(r)
            bridge["pixels"][offset + 1] = int(g)
            bridge["pixels"][offset + 2] = int(b)

    try:
        bridge["device"].set_rt_frame_socket(
            io.BytesIO(bytes(bridge["pixels"])),
            version=3,
            leds_number=led_total,
        )
    except Exception as exc:
        logging.warning("Twinkly realtime flush failed for %s: %s", host, exc)


def set_light(light, data):
    ip = light.protocol_cfg["ip"]
    led_total = int(light.protocol_cfg.get("led_total", light.protocol_cfg.get("ledCount", 150)))
    segment_idx = int(light.protocol_cfg.get("segment_index", light.protocol_cfg.get("light_nr", 0)))
    segment_count = int(light.protocol_cfg.get("segment_count", 1))

    if segment_count > 1:
        start_idx, end_idx = _segment_range_for_index(led_total, segment_idx, segment_count)
    else:
        start_idx, end_idx = 0, led_total - 1

    if "lights" in data:
        data = data["lights"]
        if isinstance(data, dict):
            # The Hue bridge can send a dict keyed by light ids, but in practice the current light's payload is the relevant part.
            data = data.get(str(light.id_v1), data.get(str(light.id_v2), data))

    rgb = _coerce_rgb(data, light)
    on = data.get("on") if isinstance(data, dict) else True
    if isinstance(data, dict) and on is False:
        rgb = [0, 0, 0]

    _paint_segment(ip, led_total, start_idx, end_idx, rgb)
    return {"status": "ok"}


def get_light_state(light):
    ip = light.protocol_cfg["ip"]
    led_total = int(light.protocol_cfg.get("led_total", light.protocol_cfg.get("ledCount", 150)))
    try:
        return requests.get(f"http://{ip}/xled/v1/gestalt", timeout=3).json()
    except Exception:
        return {"on": True, "bri": 255, "xy": [0.3, 0.3], "reachable": True, "led_total": led_total}


def generate_light_name(base_name, light_nr):
    suffix = f" {light_nr}"
    return f"{base_name[:32 - len(suffix)]}{suffix}"


def discover(detectedLights, device_ips):
    logging.debug("twinkly: <discover> invoked")
    for ip in device_ips:
        try:
            info = _get_device_state(ip)
            led_total = _get_total_leds(info)
            base_name = _get_name(info, f"Twinkly {ip}")
            if led_total <= 0:
                continue

            # Split the single physical Twinkly into multiple virtual Hue lights.
            # This gives diyHue a way to treat the string as several lights, which
            # lets you color individual ranges independently instead of the whole
            # slinger acting like one light.
            segment_count = led_total
            for idx in range(segment_count):
                start_idx, end_idx = _segment_range_for_index(led_total, idx, segment_count)
                protocol_cfg = {
                    "ip": ip,
                    "mac": info.get("mac") or ip.replace('.', ':'),
                    "led_total": led_total,
                    "segment_index": idx,
                    "segment_count": segment_count,
                    "segment_start": start_idx,
                    "segment_end": end_idx,
                    "protocol": "twinkly",
                }
                detectedLights.append({
                    "protocol": "twinkly",
                    "name": generate_light_name(base_name, idx + 1),
                    "modelid": "LST002",
                    "protocol_cfg": protocol_cfg,
                })
        except Exception as exc:
            logging.info("ip %s is not a Twinkly device: %s", ip, exc)

    return detectedLights
