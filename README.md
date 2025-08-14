# EMBRACE - **_Emb_**y **_R_**ecommendation **_a_**nd **_C_**ontext **_E_**ngine

EMBRACE is an intelligent and personalised recommendation engine for (Emby) [https://emby.media/].

# Documentation:

## Environment Variables

Environment variables are defined in the `.env` file, located at the root of the project. See the `.env.example` file for examples.

-   **`BASE_DOMAIN`**: Your Emby server's base domain. **EXPECTED FORMAT**: `https://[domain]/emby` - e.g. `https://192.168.x.x:[PORT]/emby`, `https://myembyserver.mydomain.com/emby`.

-   **`EMBY_API_KEY`**: Your Emby server's API key. You can obtain this by going to: `[Your Emby Server URL]/web/index.html#!/dashboard` -> scroll down, at the bottom of the page is a link labelled "API". This will take you to the Swapper API webpage for your server.

-   **`ENVIRONMENT`**: Specifies the runtime environment, values: "dev" | "staging" | "prod".

-   **`SQLITE_DB_NAME`**: (Optional) Name of the SQLite database. **Default is "EMBRACE_SQLITE_DB.db"**.
