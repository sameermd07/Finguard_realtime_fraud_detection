# Alert sinks: one email per row of each gold alert table (Gmail SMTP over SSL).
# The Gmail app password is read from the Databricks secret scope, never hard-coded.
# Sink decorator names (foreach_batch_sink / append_flow) can differ slightly between
# Databricks releases; if the pipeline rejects them, check the current SDP docs.
from pyspark import pipelines as dp
import smtplib
from email.mime.text import MIMEText

FROM = "your.email@gmail.com"  # the Gmail account that created the app password
APP_PASSWORD = dbutils.secrets.get(scope="finguard-scope", key="gmail-app-password")


def send_email(to, subject, html):
    msg = MIMEText(html, "html")
    msg["Subject"], msg["From"], msg["To"] = subject, FROM, to
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(FROM, APP_PASSWORD)
        server.sendmail(FROM, [to], msg.as_string())


# ---------- High-value transaction alerts ----------
def create_alert_email_body(d):
    return f"""
    <h2>Fraud alert: transaction above your limit</h2>
    <p>Hello {d['customer_name']},</p>
    <p>We detected a transaction of <b>{d['transaction_amount']} {d['currency']}</b>
       at <b>{d['merchant_name']}</b> ({d['city']}, {d['country']}), above your configured
       limit of <b>{d['transaction_limit']}</b>.</p>
    <p>Transaction ID: {d['transaction_id']}<br>Time: {d['transaction_timestamp']}</p>
    <p>If this was you, no action is needed. Otherwise report it immediately.</p>
    """


@dp.foreach_batch_sink(name="email_notifier_sink")
def email_notifier_sink(df, batch_id):
    for row in df.collect():
        d = row.asDict()
        try:
            send_email(
                d["customer_email"],
                f"Fraud alert: high value transaction {d['transaction_id']}",
                create_alert_email_body(d),
            )
        except Exception as e:
            print("email failed:", e)


@dp.append_flow(target="email_notifier_sink")
def email_flow():
    return spark.readStream.table("finguard.gold.high_value_transactions_alert")


# ---------- Watch-listed card alerts ----------
def create_fraud_card_email_body(d):
    return f"""
    <h2>Fraud alert: card on watch list</h2>
    <p>Hello {d['customer_name']},</p>
    <p>A transaction of <b>{d['transaction_amount']} {d['currency']}</b> at
       <b>{d['merchant_name']}</b> was made on a card flagged as
       <b>{d['risk_level']}</b> risk ({d['reason_description']}).</p>
    <p>Transaction ID: {d['transaction_id']}<br>Watch list ID: {d['watchlist_id']}</p>
    <p>Action on the card: {d['action']}. Please contact your bank immediately.</p>
    """


@dp.foreach_batch_sink(name="fraud_card_email_sink")
def fraud_card_email_sink(df, batch_id):
    for row in df.collect():
        d = row.asDict()
        try:
            send_email(
                d["customer_email"],
                f"Fraud alert: watch-listed card {d['transaction_id']}",
                create_fraud_card_email_body(d),
            )
        except Exception as e:
            print("email failed:", e)


@dp.append_flow(target="fraud_card_email_sink")
def fraud_card_email_flow():
    return spark.readStream.table("finguard.gold.fraud_card_alerts")
