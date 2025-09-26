from dotenv import load_dotenv

class Notifications:
    """
    A utility class for sending notifications out to the supported services:
    - Discord webhooks
    - More TBA...
    """
    
    def discord_send_webhook(self, ) -> bool:
        """
        Sends a POST request to the discord webhook with the specified message. Requires the `DISCORD_WEBHOOK_URL` environment variable.
        Args:
            message: The JSON body to send as the webhook message
        Returns:
            (bool): True if POST request receives 'ok' response, otherwise false
        """
        
        return False