import requests
import time
import sys
from urllib.parse import urljoin

# IDs para testear - añade los que quieras aquí
STREAM_IDS = [
  "911ad127726234b97658498a8b790fdd7516541d",
  "6e1b5fd8753352486aa932f802534a17556e1f60",
  "ad42faa399df66dcd62a1cbc9d1c99ed4512d3b8"
]

BASE_URL = "https://acestream.walerike.com"


def check_engine():
  try:
    response = requests.get(f"{BASE_URL}/webui/api/service", timeout=5)
    if response.status_code == 200:
      print("✓ Acestream Engine corriendo\n")
      return True
  except:
    pass
  print("✗ Acestream Engine no está corriendo")
  sys.exit(1)


def test_stream(stream_id):
  print(f"{'=' * 60}")
  print(f"ID: {stream_id}")
  stream_url = f"{BASE_URL}/ace/getstream?id={stream_id}"

  try:
    response = requests.get(stream_url, timeout=15, stream=True)

    if response.status_code == 200:
      bytes_received = 0
      for i, chunk in enumerate(response.iter_content(chunk_size=8192)):
        if chunk:
          bytes_received += len(chunk)
        if i >= 5:
          break

      response.close()

      if bytes_received > 0:
        print(f"✓ VÁLIDO - {bytes_received} bytes recibidos")
        return True
      else:
        print("✗ INVÁLIDO - Sin datos")
        return False
    else:
      print(f"✗ INVÁLIDO - Status {response.status_code}")
      return False

  except requests.exceptions.Timeout:
    print("✗ INVÁLIDO - Timeout")
    return False
  except Exception as e:
    print(f"✗ ERROR - {e}")
    return False


def main():
  print("ACESTREAM ID TESTER")
  print("=" * 60)
  check_engine()

  results = {}
  total = len(STREAM_IDS)

  for i, stream_id in enumerate(STREAM_IDS, 1):
    print(f"[{i}/{total}]")
    results[stream_id] = test_stream(stream_id)
    if i < total:
      time.sleep(1)

  print(f"\n{'=' * 60}")
  print("RESUMEN")
  print("=" * 60)
  valid = sum(results.values())
  print(f"Total: {total} | Válidos: {valid} | Inválidos: {total - valid}\n")

  for stream_id, is_valid in results.items():
    print(f"{'✓' if is_valid else '✗'} {stream_id}")


if __name__ == "__main__":
  main()