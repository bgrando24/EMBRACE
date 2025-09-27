from dotenv import load_dotenv
import os
import json
from urllib import error, request

class Notifications:
    """
    A utility class for sending notifications out to the supported services:
    - Discord webhooks
    - More TBA...
    """

    def __init__(self):
        self.DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
    

    def discord_send_webhook(self, message: str) -> bool:
        """
        Sends a POST request to the discord webhook with the specified message. Requires the `DISCORD_WEBHOOK_URL` environment variable.
        Args:
            message: The JSON body to send as the webhook message
        Returns:
            (bool): True if POST request receives 'ok' response, otherwise false
        """

        if not self.DISCORD_WEBHOOK_URL:
            print(f"ERROR [discord_send_webhook]: Environment variable 'DISCORD_WEBHOOK_URL' not found!")
            return False

        payload = json.dumps({"content": message}).encode("utf-8")
        req = request.Request(
            self.DISCORD_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            response = request.urlopen(req, timeout=10)
            return response.getcode() >= 200 and response.getcode() < 300
        except error.URLError as exc:
            print(f"[WARN] Discord notification failed: {exc}")

        return False
