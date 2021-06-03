import os
import json
import socket
import base64
import pickle
import logging
import mimetypes
from airflow.models import Variable
from airflow import AirflowException
from email.encoders import encode_base64
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_tz, mktime_tz
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

socket.setdefaulttimeout(600)  # set timeout to 10 minutes

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s  File:%(filename)s  Function:%(funcName)s()  Line:%(lineno)d  Msg: %(message)s'
)


# https://developers.google.com/gmail/api/quickstart/python
class Gmail:
    def __init__(self, download_path='_data',
                 credentials_file='credentials.json',
                 token_file='token.pickle',
                 credentials_var='olaplex_reports_gmail_api'):
        self.download_path = download_path
        self.scopes = ['https://www.googleapis.com/auth/gmail.readonly',
                       'https://www.googleapis.com/auth/gmail.modify']
        self.token_file = token_file
        self.credentials_file = credentials_file
        self.credentials_var = credentials_var
        self.logger = self._get_logger()
        self.service = self._setup_connection()

    def _get_logger(self):
        logger = logging.getLogger(self.__str__())
        return logger

    def _get_credentials(self, credentials_json=None):
        """
        Create JSON credentials file.
        If credentials not provided as parameter, pull from Airflow variable
        """
        if not os.path.exists(self.credentials_file):
            if not credentials_json:
                credentials_json = Variable.get(self.credentials_var)
            else:
                credentials_json = json.dumps(credentials_json)

            with open(self.credentials_file, 'w') as f:
                self.logger.info(f"Writing credentials in {self.credentials_file}")
                f.write(credentials_json)

    def _setup_connection(self):
        self._get_credentials()

        # The pickle file (self.token_file) stores the user's access and refresh tokens, and
        # is created automatically when the authorization flow completes for the first time.
        creds = None
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, self.scopes)
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
        service = build('gmail', 'v1', credentials=creds,cache_discovery=False)
        return service

    def _get_labels(self):
        results = self.service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        label_names = []
        for label in labels:
            label_names.append(label['name'])
        return label_names

    def _get_metadata(self, message):
        headers = message['payload']['headers']
        result = {}
        result['ID'] = message['id']
        meta_fields = ['Message-ID', 'Subject', 'From', 'To', 'Date']

        for field in meta_fields:
            for item in headers:
                if item['name'].lower() == field.lower():
                    result[field] = item['value']

        if result['Date'] is not None:
            result["Timestamp"] = mktime_tz(parsedate_tz(result['Date']))
        return result

    def _generate_full_path(self, message, file_name):
        data_path = os.path.join(os.getcwd(), self.download_path)
        if not os.path.exists(data_path):
            self.logger.info("Creating {} folder".format(self.download_path))
            os.makedirs(data_path, exist_ok=True)

        metadata = self._get_metadata(message)
        # remove unnessary prefix from subject line
        subject = metadata['Subject'].replace("[EXTERNAL SENDER]", "").strip()
        subject = '_'.join(subject.split(" "))
        ts = metadata['Timestamp']
        full_path = f"{data_path}/{subject}_{ts}_{file_name}"
        return full_path

    def _mark_as_seen(self, message):
        self.logger.info(f"Marking message with id: {message['id']} as seen")
        result = self.service.users().messages().modify(userId='me',
                                                        id=message['id'],
                                                        body={'removeLabelIds': ['UNREAD']}
                                                        ).execute()

    def _yield_messages(self, search_label='AA-Test', search_criteria='is:unread'):

        if search_label not in self._get_labels():
            self.logger.error("Not a valid search label")
            return

        query = f'in:{search_label} {search_criteria}'
        results = self.service.users().messages().list(userId='me', q=query).execute()
        num_results = results['resultSizeEstimate']

        self.logger.info(f"found {num_results} matching emails")
        if num_results == 0:
            return
        messages = results.get('messages', [])

        for message in messages:
            msg = self.service.users().messages().get(
                userId='me', id=message['id'], format='full').execute()
            yield msg
    def get_email_text(self, search_label='AA-Test', search_criteria='is:unread', mark_as_seen=False):
        result = []
        for message in self._yield_messages(search_label, search_criteria):
            parsed_content = {}
            parsed_content['Content'] = message.get('snippet')
            for k, v in self._get_metadata(message).items():
                parsed_content[k] = v
            result.append(parsed_content)

            if mark_as_seen:
                self._mark_as_seen(message)
        return result           #this method is different

    def get_email_attachment(self, search_label='AA-Test', search_criteria='is:unread',
                             extensions=['csv', 'xlsx'], mark_as_seen=False):
        path_list = []
        for message in self._yield_messages(search_label, search_criteria):
            for part in message['payload'].get('parts', ''):
                if part['filename']:
                    data = None
                    file_name = part['filename']
                    # if file extension don't match skip
                    if file_name.split(".")[-1] not in extensions:
                        self.logger.info(f"Skipping file {file_name}. Not in expected format.")
                        continue

                    if 'data' in part['body']:
                        data = part['body']['data']

                    else:
                        att_id = part['body']['attachmentId']
                        att = self.service.users().messages().attachments().get(
                            userId='me',
                            messageId=message.get('id'),
                            id=att_id).execute()
                        data = att['data']

                    full_path = self._generate_full_path(message, file_name)
                    if not os.path.isfile(full_path):
                        with open(full_path, 'wb') as f:
                            f.write(base64.urlsafe_b64decode(data.encode('UTF-8')))
                            self.logger.info("Data written to file {}".format(full_path))
                            path_list.append(full_path)
                    else:
                        self.logger.info(f"File {full_path} already exists. Skipping download.")

            if mark_as_seen: #new added line
                self._mark_as_seen(message)
        return path_list

    def get_attachment_with_metadata(self, search_label='AA-Test',
                                     search_criteria='is:unread', extensions=['csv', 'xlsx'], mark_as_seen=False):
        result = []
        for message in self._yield_messages(search_label, search_criteria):
            for part in message['payload'].get('parts', ''):
                if part['filename']:
                    data = None
                    file_name = part['filename']
                    # if file extension don't match skip

                    if file_name.split(".")[-1] not in extensions:
                        self.logger.info(f"Skipping file {file_name}. Not in expected format.")
                        continue

                    if 'data' in part['body']:
                        data = part['body']['data']

                    else:
                        att_id = part['body']['attachmentId']
                        att = self.service.users().messages().attachments().get(
                            userId='me',
                            messageId=message.get('id'),
                            id=att_id).execute()
                        data = att['data']

                    metadata = self._get_metadata(message)
                    data_path = os.path.join(os.getcwd(), self.download_path)
                    if not os.path.exists(data_path):
                        self.logger.info("Creating {} folder".format(data_path))
                        os.makedirs(data_path, exist_ok=True)

                    full_path = os.path.join(data_path, f"{metadata['Timestamp']}_{file_name}")
                    metadata['local_path'] = full_path
                    if not os.path.isfile(full_path):
                        with open(full_path, 'wb') as f:
                            f.write(base64.urlsafe_b64decode(data.encode('UTF-8')))
                            self.logger.info("Data written to file {}".format(full_path))
                            result.append(metadata)
                    else:
                        self.logger.info(f"File {full_path} already exists. Skipping download.")
            if mark_as_seen:
                self._mark_as_seen(message)
        return result

    def _create_email_message(self, **kwargs):

        message = MIMEMultipart()

        msg = MIMEText(kwargs['message_text'])
        message.attach(msg)

        message['to'] = kwargs['to_email']
        message['from'] = kwargs['from_email']
        message['subject'] = kwargs['subject']

        if 'cc_email' in kwargs:
            message['cc'] = kwargs['cc_email']

        files = []
        if 'attachment' in kwargs and kwargs['attachment'] is not None:
            files = kwargs['attachment']

        for file in files:
            content_type, encoding = mimetypes.guess_type(file)

            if content_type is None or encoding is not None:
                content_type = 'application/octet-stream'

            main_type, sub_type = content_type.split('/', 1)
            if main_type == 'text':
                with open(file, 'r') as fp:
                    msg = MIMEText(fp.read(), _subtype=sub_type)

            elif main_type == 'image':
                with open(file, 'rb') as fp:
                    msg = MIMEImage(fp.read(), _subtype=sub_type)

            elif main_type == 'audio':
                with open(file, 'rb') as fp:
                    msg = MIMEAudio(fp.read(), _subtype=sub_type)
            else:
                with open(file, 'rb') as fp:
                    msg = MIMEBase(main_type, sub_type)
                    msg.set_payload(fp.read())

            filename = os.path.basename(file)
            encode_base64(msg)
            msg.add_header('Content-Disposition','attachment', filename=filename)
            message.attach(msg)

        raw_message = base64.urlsafe_b64encode(message.as_string().encode("utf-8"))
        return {
            'raw': raw_message.decode("utf-8")
        }

    def send_email(self, from_email, to_email, subject, message_text,
                   attachment=None, cc_email=None):

        msg = self._create_email_message(from_email=from_email,
                                         to_email=to_email,
                                         subject=subject,
                                         message_text=message_text,
                                         attachment=attachment,
                                         cc_email=cc_email)
        try:
            message = self.service.users().messages().send(userId='me', body=msg).execute()
            self.logger.info(f"Sent email with Message ID: {message['id']}")
            return message
        except HttpError as e:
            self.logger.error(f"An error occured {e}")
            raise AirflowException(e)
