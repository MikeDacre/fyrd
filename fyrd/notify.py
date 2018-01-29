# -*- coding: utf-8 -*-
"""
User notification using email, either Linux or SMTP.

Must be configured in the primary config file.

Primary function
----------------
notify(msg, to, subject)
"""
import os as _os
import smtplib as _smtplib
import subprocess as _sub

from six.moves import email_mime_multipart as _multi
from six.moves import email_mime_text as _mtxt

from fyrd import run as _run
from fyrd import conf as _conf
from fyrd import logme as _logme


def notify(msg, to=None, subject='Fyrd notification'):
    """Send a message."""
    if not to:
        to = _conf.get_option('notify', 'notify_address')
    if not to:
        _logme.log('No notification address, cannot send notification',
                   'error')
        return False
    assert isinstance(msg, str)
    assert isinstance(to, str)
    assert isinstance(subject, str)
    opt = _conf.get_option('notify', 'mode', None)
    if opt == 'linux':
        return mail(msg, to, subject)
    elif opt == 'smtp':
        return smtp(msg, to, subject)
    raise ValueError('Unknown mail mode {}'.format(opt))


def mail(msg, to, subject):
    """Send a message with linux mail."""
    _logme.log('Sending with sendmail', 'debug')
    if not _run.which('mail'):
        _logme.log('Linux mail not found, cannot send notification' +
                   'perhaps configure SMTP?', 'error')
        return False
    job = _sub.run(
        ['mail', '-s', subject, to],
        input=msg.encode(),
        stdout=_sub.PIPE, stderr=_sub.PIPE
    )
    if not job.returncode == 0:
        raise _sub.CalledProcessError(
            cmd='mail -s {} {}'.format(subject, to),
            output=job.stdout, stderr=job.stderr,
            returncode=job.returncode
        )


def smtp(msg, to, subject):
    """Send a message via smtp."""
    _logme.log('Sending with smtp', 'debug')
    # Get Options
    host = _conf.get_option('notify', 'smtp_host', None)
    if not host:
        raise ValueError('smtp_host must be specified in the config')
    port = int(_conf.get_option('notify', 'smtp_port', 587))
    tls = _conf.get_option('notify', 'smtp_tls', True)
    fromaddr = _conf.get_option('notify', 'smtp_from', None)
    if not fromaddr:
        raise ValueError('smtp_from must be specified in the config')
    user = _conf.get_option('notify', 'smtp_user', None)
    if not user:
        user = fromaddr
    passfl = _conf.get_option('notify', 'smtp_passfile', None)
    if not passfl:
        raise ValueError('smtp_passfl must be specified in the config')
    with open(_os.path.expanduser(passfl)) as fin:
        passwd = fin.readline().strip()

    # Prepare Message
    body = msg
    msg = _multi.MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = to
    msg['Subject'] = subject

    msg.attach(_mtxt.MIMEText(body, 'plain'))

    server = _smtplib.SMTP(host, port)
    if tls:
        server.starttls()
    server.login(user, passwd)
    text = msg.as_string()
    server.sendmail(fromaddr, to, text)
    server.quit()
