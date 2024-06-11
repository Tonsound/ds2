from decouple import config
import os
from flask import jsonify

class Config(object):
    CSRF_ENABLED = True
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'None'
    MAIL_SERVER = 'smtp.gmail.com'
    MAIL_PORT = 465
    MAIL_USE_TLS = False
    MAIL_USE_SSL = True
    MAIL_USERNAME = os.environ.get("GMAIL_USER")
    MAIL_PASSWORD = os.environ.get("GMAIL_PASSWORD")
    CREDS = jsonify(os.environ.get("CREDS"))
    API_KEY_CLP = os.environ.get("API_KEY_CLP")
    CLIENT_ID_SDS = os.environ.get("CLIENT_ID_SDS")
    CLIENT_SECRET_SDS = os.environ.get("CLIENT_SECRET_SDS")
    CLIENT_ID = os.environ.get("CLIENT_ID")
