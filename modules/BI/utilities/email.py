from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from errno import ECONNREFUSED
from itertools import chain
from mimetypes import guess_type
from os.path import abspath, basename, expanduser
from smtplib import SMTP
from socket import error as socket_error
from subprocess import Popen, PIPE

from . import *


# %% Emailer
class Email:
    def __init__(self, to, subject, template,
                 from_email=None, bcc=list(), cc=list(),
                 username='default', password=None, file_names=list(),
                 html=False, html_format_params=False, image=None, css=None):
        if not isinstance(to, list):
            try:
                self.to = to.split(',')
            except Exception as e:
                raise e
        else:
            self.to = to

        self.cc = cc
        self.bcc = bcc

        if username != 'default':
            self.username = username
        else:
            self.username = 'jonathan.lauret@vivintsolar.com'

        if password:
            self.password = password
        else:
            self.password = os.environ.get('JD_EMAIL_PASS')

        if from_email is None:
            self.sender = self.username
        else:
            self.sender = from_email

        self.file_names = file_names

        self.files = []
        if self.file_names:
            self.get_files()
        else:
            self.files = []

        self.subject = subject
        self.template = template
        self.html = html
        self.html_format_params = html_format_params
        self.image = image
        self.css = css

        self.recipients = list(chain(self.to, self.cc, self.bcc))

    def get_files(self):
        for file_name in self.file_names:
            file_dir = os.path.dirname(file_name)
            if file_name in self.file_names:
                self.files.append(os.path.join(file_dir, file_name))

    def valid_email(self, email):
        if len(email) > 7:
            if re.match("^.+@([?)[a-zA-Z0-9-.])+.([a-zA-Z]{2,3}|[0-9]{1,3})(]?)$", email) != None:
                return True
            return False

    def get_mimetype(self, filename):
        """Returns the MIME type of the given file.

            :param filename: A valid path to a file
            :type filename: str

            :returns: The file's MIME type
            :rtype: tuple
            """

        content_type, encoding = guess_type(filename)
        if content_type is None or encoding is not None:
            content_type = "application/octet-stream"
        return content_type.split("/", 1)

    def mimify_file(self, filename):
        """Returns an appropriate MIME object for the given file.

        :param filename: A valid path to a file
        :type filename: str

        :returns: A MIME object for the givne file
        :rtype: instance of MIMEBase
        """

        filename = abspath(expanduser(filename))
        base_file_name = basename(filename)

        msg = MIMEBase(*self.get_mimetype(filename))
        msg.set_payload(open(filename, "rb").read())
        msg.add_header("Content-Disposition", "attachment", filename=base_file_name)

        encode_base64(msg)

        return msg

    def build_msg(self):
        """Create outgoing email with the given parameters.

        This function assumes your system has a valid MTA (Mail Transfer Agent)
        or local SMTP server. This function will first try a local SMTP server
        and then the system's MTA (/usr/sbin/sendmail) connection refused.
        """

        format_html = False

        if self.html_format_params:
            format_html = True
            format_subject = self.subject.format(**self.html_format_params)
        else:
            format_subject = self.subject

        # Prepare Message
        self.msg = MIMEMultipart()
        self.msg.preamble = format_subject
        self.msg.add_header("From", self.sender)
        self.msg.add_header("Subject", format_subject)
        self.msg.add_header("To", ", ".join(self.to))
        self.cc and self.msg.add_header("Cc", ", ".join(self.cc))

        # Attach the main text
        if self.html:
            with open(self.template, encoding='utf8') as html_file:
                html_body = ''.join(html_file.readlines())
                if self.css is not None:
                    with open(self.css, encoding='utf8') as css_file:
                        css_insert = ''.join(css_file.readlines())
                else:
                    css_insert = ''
                if format_html:
                    html_body = html_body.format(**self.html_format_params, css_text=css_insert)
            self.msg.attach(MIMEText(html_body, 'html'))
        else:
            self.msg.attach(MIMEText(self.template))

        if self.image is not None:
            fp = open(self.image, 'rb')
            msg_image = MIMEImage(fp.read())
            fp.close()
            msg_image.add_header('Content-ID', '<image1>')
            self.msg.attach(msg_image)

        # Attach any files
        [self.msg.attach(self.mimify_file(filename)) for filename in self.files]

    def send_msg(self):
        # Contact SMTP server and send Message
        self.build_msg()
        try:
            smtp = SMTP('smtp.gmail.com', 587)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(self.username, self.password)
            smtp.sendmail(self.sender, self.recipients, self.msg.as_string())
            smtp.quit()
        except socket_error as e:
            if e.args[0] == ECONNREFUSED:
                p = Popen(["/usr/sbin/sendmail", "-t"], stdin=PIPE)
                p.communicate(self.msg.as_string())
            else:
                raise
