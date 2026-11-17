# Databricks notebook source
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.secrets.put_secret(scope="finguard-scope", key="gmail-app-password",
                     string_value="<GMAIL_APP_PASSWORD>")

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText

FROM = "your.email@gmail.com"      # the account that created the app password
TO = "your.email@gmail.com"
pw = dbutils.secrets.get(scope="finguard-scope", key="gmail-app-password")

msg = MIMEText("<h3>Test email from Databricks</h3>", "html")
msg["Subject"], msg["From"], msg["To"] = "Test email", FROM, TO

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
    s.login(FROM, pw)
    s.sendmail(FROM, [TO], msg.as_string())
print("Email sent successfully")
