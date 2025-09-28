from dotenv import load_dotenv
import os
import json
import requests
from urllib import error, request

class Notifications:
    """
    A utility class for sending notifications out to the supported services:
    - Discord webhooks
    - More TBA...
    """

    def __init__(self):
        load_dotenv()
        self.DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
    

    def discord_send_webhook(self, message: str) -> bool:
        """
        Send a message to a Discord webhook.
        Returns True on success, False otherwise.
        """
        if not self.DISCORD_WEBHOOK_URL:
            print("ERROR [discord_send_webhook]: 'DISCORD_WEBHOOK_URL' not set")
            return False

        if not isinstance(message, str):
            message = str(message)

        # Discord hard limit: 2000 chars for 'content'
        if len(message) > 2000:
            print(f"[WARN] Truncating message from {len(message)} to 2000 characters")
            message = message[:2000]

        payload = {"content": message}
        print(f"Sending webhook to: {self.DISCORD_WEBHOOK_URL}")
        print(f"Payload: {json.dumps(payload)}")

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "EMBRACE/cron (+contact@example.com)",
            "Accept": "application/json",
        }

        response = None
        try:
            response = requests.post(
                self.DISCORD_WEBHOOK_URL,
                json=payload,
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError as exc:
            resp = exc.response or response
            if resp is not None:
                print(
                    f"ERROR [discord_send_webhook]: HTTP {resp.status_code} {resp.reason}\n"
                    f"Body: {resp.text[:500]}"
                )
            else:
                print(f"ERROR [discord_send_webhook]: HTTP error without response: {exc}")
        except requests.exceptions.RequestException as exc:
            print(f"ERROR [discord_send_webhook]: Request failed: {exc}")

        return False
