import base64
import zlib
import json


# 👇 Pega aquí el contenido del campo "state" del JSON
STATE_B64 = """
eNqFkE9PwzAMxb9LrpSpSydBK3HagAObBhLsgqYorF4bkaZV7MLKxHcn2R8tVBPk9n5+dp69ZQJJEogPsKhqwzIeHRGYQpmgMooYrkqopDCyAmTZK6vqvNUwEoWisn0TVuWOLyOWS5IItDOyjJmO5EYxx2EtW00imOPKZ6dErFENaJ/gD5s4mnbDkZSR5MIK6hrfkmsaBBgHa6UBOySoeg37P0yrtdv/sLIoJZZuysVkQU+Pi9mUxuV18TVPUj2/ipW178XdbRrfP7xM9fMCN+PJ7MaNxbq1K3+f7fnNHLcQmDrQuv4UZFXj7+ZRY2tXRMhFA1bV+e7YBx+Ph+llPHQf/QK8D5IQ8LjX4gHvg4QtvyNWWADzf5y97ZQm1Lynk0Afo4Sa97QP4t8PJCjkCQ==
""".strip()


def decode_dlt_state(encoded_state: str) -> dict:
    """Decodifica el state comprimido de dlt (base64 + zlib)."""
    decoded_bytes = base64.b64decode(encoded_state)
    decompressed = zlib.decompress(decoded_bytes)
    return json.loads(decompressed.decode("utf-8"))


def find_key(obj, key, path=""):
    """Busca recursivamente una clave dentro del JSON."""
    results = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            if k == key:
                results.append((new_path, v))
            results.extend(find_key(v, key, new_path))

    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_path = f"{path}[{i}]"
            results.extend(find_key(v, key, new_path))

    return results


if __name__ == "__main__":

    state_obj = decode_dlt_state(STATE_B64)

    print("\n=== STATE COMPLETO DECODIFICADO ===\n")
    print(json.dumps(state_obj, indent=2))

    print("\n=== BUSCANDO processed_periods ===\n")
    matches = find_key(state_obj, "processed_periods")

    if matches:
        for path, value in matches:
            print(f"\nEncontrado en: {path}")
            print("Valor:", value)
    else:
        print("No se encontró 'processed_periods' en el state.")
