# Google Login Website

A simple Flask website with Google OAuth2 authentication.

## Setup Instructions

1. Create a project in the Google Cloud Console (https://console.cloud.google.com/)
2. Enable the Google+ API
3. Create OAuth 2.0 credentials (OAuth client ID)
   - Application type: Web application
   - Authorized redirect URIs: http://127.0.0.1:5000/callback

4. Download the client secret JSON file and save it as `client_secret.json` in the project root

5. Create a `.env` file in the project root with the following contents:
```
GOOGLE_CLIENT_ID=your_client_id_here
SECRET_KEY=your_secret_key_here
```

6. Install the required packages:
```bash
pip install -r requirements.txt
```

7. Run the application:
```bash
python app.py
```

8. Visit http://127.0.0.1:5000 in your browser

## Features
- Google OAuth2 authentication
- Protected routes
- User session management
- Modern UI with Tailwind CSS
