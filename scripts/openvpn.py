import subprocess
import time
import requests

class OpenVpn:
  def __init__(self, config_path="japan.ovpn"):
    self.config_path = config_path
    self.process = None

  def start_openvpn(self):
    try:
      self.process = subprocess.Popen(
          ['sudo', 'openvpn', '--config', self.config_path],
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE
      )
      print("[🔄] OpenVPN starting...")
    except Exception as e:
      print(f"[❌] Failed to start OpenVPN: {e}")

  @staticmethod
  def get_public_ip():
    try:
      ip = requests.get('https://api.ipify.org', timeout=10).text
      print(f"[🌐] Public IP: {ip}")
      return ip
    except requests.RequestException as e:
      print(f"[❌] Could not fetch IP: {e}")
      return None

  def stop_openvpn(self):
    if self.process:
      self.process.terminate()
      print("[⛔] OpenVPN terminated.")

