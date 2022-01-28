import smtplib, ssl, os

DOMAIN = "fable.eecs.umich.edu/"

def sendEmail(rec, article_url, article_url_title):

    # For links that come from our auto crawler
    if not str(rec):
        return

    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "anishnya@gmail.com"
    receiver_email = str(rec)
    password = str(os.getenv("EMAIL_PASSWORD"))

    output_url= DOMAIN + str(article_url_title)

    message = """\
    Subject: Fable-Bot Completed on {0}

    The results can be found on {1}""".format(article_url, output_url)


    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)