import logging
import sys
import typing

import pandas as pd
import clickhouse_driver
from typing import Union
from collections.abc import Callable
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime


class ClickHouseIO:
    @staticmethod
    def get_data(request: str, source_server: str, **kwargs) -> Union[pd.DataFrame, None]:
        """
        Parameters:
        -----------
        request : SQL request String
        source_server : ClickHouse server address

        Returns:
        --------
        Pandas DataFrame
        """
        try:
            client = clickhouse_driver.Client(source_server, **kwargs)
            query_result = client.execute(request,
                                          with_column_types=True)
            fields_list = [column_data[0] for column_data in query_result[1]]
            data = pd.DataFrame(query_result[0], columns=fields_list)
        except (clickhouse_driver.errors.ServerException,
                clickhouse_driver.errors.SocketTimeoutError,
                clickhouse_driver.errors.NetworkError,
                clickhouse_driver.errors.Error,
                EOFError):
            logging.debug("ClickHouseIO.get_data: failure")
            return None
        return data

    @staticmethod
    def get_raw_data(request: str, source_server: str, **kwargs) -> typing.Tuple:
        """
        Parameters:
        -----------
        request : SQL request String
        source_server : ClickHouse server address

        Returns:
        --------
        list
        """
        try:
            client = clickhouse_driver.Client(source_server, **kwargs)
            query_result = client.execute(request,
                                          with_column_types=True)
            data = query_result[0]
            columns = [column_data[0] for column_data in query_result[1]]
        except (clickhouse_driver.errors.ServerException,
                clickhouse_driver.errors.SocketTimeoutError,
                clickhouse_driver.errors.NetworkError,
                clickhouse_driver.errors.Error,
                EOFError):
            logging.debug("ClickHouseIO.get_data: failure")
            return None, None
        return data, columns

    @staticmethod
    def put_data(data: pd.DataFrame, table_name: str, source_server: str, **kwargs) -> None:
        """
        Parameters:
        -----------

        data : pandas DataFrame
        table_name : ClickHouse table full name
        source_server : ClickHouse server address
        """
        if data is None or len(data.index) == 0:
            logging.debug("ClickHouseIO.put_data: try insert empty data")
            return

        try:
            clickhouse_driver.Client(source_server, **kwargs).execute(
                    """
                    INSERT INTO {} VALUES
                    """.format(table_name),
                    data.to_dict('records'))
        except (clickhouse_driver.errors.ServerException,
                clickhouse_driver.errors.Error,
                clickhouse_driver.errors.NetworkError,
                clickhouse_driver.errors.SocketTimeoutError):
            logging.exception("ClickHouseIO.put_data: failed "
                              "for '%s'", table_name)

    @staticmethod
    def put_dict(data: list, table_name: str, source_server: str, values: list, **kwargs) -> None:
        """
        Parameters:
        -----------

        data : dict by rows
        table_name : ClickHouse table full name
        source_server : ClickHouse server address
        values: list
        """
        if data is None or len(data) == 0:
            logging.debug("ClickHouseIO.put_data: try insert empty data")
            return

        try:
            clickhouse_driver.Client(source_server, **kwargs).execute(
                    """
                    INSERT INTO {} ({}) VALUES
                    """.format(table_name,
                               ', '.join(values)),
                    data, types_check=True)
        except (clickhouse_driver.errors.ServerException,
                clickhouse_driver.errors.Error,
                clickhouse_driver.errors.NetworkError,
                clickhouse_driver.errors.SocketTimeoutError) as e:
            logging.exception(e)

    @staticmethod
    def truncate_table(table_name: str, source_server: str, **kwargs) -> None:
        request = """
        TRUNCATE TABLE {table_name}
        """.format(table_name=table_name)
        try:
            clickhouse_driver.Client(source_server, **kwargs).execute(request)
        except (clickhouse_driver.errors.ServerException,
                clickhouse_driver.errors.SocketTimeoutError,
                clickhouse_driver.errors.NetworkError,
                clickhouse_driver.errors.Error,
                EOFError):
            logging.exception("ClickHouseIO.truncate_table has been failed "
                              "for '%s' [%s]", table_name, source_server)

    @staticmethod
    def request(source_server: str, request: str, **kwargs) -> None:
        try:
            clickhouse_driver.Client(source_server, **kwargs).execute(request)
        except (clickhouse_driver.errors.ServerException,
                clickhouse_driver.errors.SocketTimeoutError,
                clickhouse_driver.errors.NetworkError,
                clickhouse_driver.errors.Error,
                EOFError):
            logging.exception("ClickHouseIO.truncate_table has been failed "
                              "for %s", source_server)


class KafkaIO:
    def __init__(self, kafka_server: str, topic: str):
        """
        Initialize with default topic and server from settings.
        IMPORTANT: JSON as value_serializer in producer!
        Main method for sending is <send_as_json>

        # Usage producer:
        kafka = KafkaIO(kafka_server, topic)
        kafka.send_as_json(json)
        ...
        kafka.flush()
        ...
        kafka.close()

        # Usage consumer:
        kafka = KafkaIO(io_settings)
        kafka.subscribe()
        # endless loop:
        for message in kafka:
            print(message.value['ip'],
                  message.value['rating'])
        """
        import json

        self._server = kafka_server
        self._topic = topic
        self._producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'),
                                       bootstrap_servers=self._server)
        self._consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                       bootstrap_servers=self._server)

    def write(self, json: Union[str, dict]) -> None:
        self.send_as_json(json)

    def subscribe(self, topic: Union[str, None] = None) -> None:
        self._consumer.subscribe(topic if topic is not None else self._topic)

    def get_consumer(self) -> KafkaConsumer:
        return self._consumer

    def send_as_json(self, json: Union[str, dict], topic: typing.Union[str, None] = None,
                     key: typing.Union[str, None] = None) -> None:
        # async sending:
        self._producer.send(self._topic if topic is None else topic, json, key=key)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()

    @staticmethod
    def ratings(key: str) -> int:
        ratings = {"clean": 0,
                   "dirty": 1,
                   "dangerous": 2}
        try:
            value = ratings[key]
        except KeyError:
            logging.error("KafkaIO.ratings failed: no such key '%s' in "
                          "default dict %s", key, ratings.keys())
            raise ValueError("Bad key in KafkaIO.ratings")
        else:
            return value

    def send_rating_to_kafka(self, data: pd.DataFrame, rating: int,
                             event_id: str = 'DEFAULT', use_key: bool = False) -> None:
        """
        Send to Kafka by default, call for new messages

        Parameters:
        -----------
        data : pandas.DataFrame with column 'ip'
        rating : int number in set S = {0, 1, 2}
        event_id : String identifier by instance
        """
        from kafka.errors import KafkaError
        from wetllib.scheduler import _to_str

        timestamp = _to_str(datetime.today())
        try:
            for row in data.itertuples(index=False):
                self.send_as_json(dict(timestamp=timestamp,
                                       ip=str(row.ip),
                                       rating=rating,
                                       event_id=event_id),
                                  key=bytearray(str(row.ip).encode('utf-8')) if use_key else None)
            self.flush()
            self.close()
        except KafkaError as error:
            logging.debug('send_rating_to_kafka: failed by KafkaError %s', type(error))

    def notify_about_an_event(self, event_id: str, times: list, addition: Union[str, None, list] = None) -> None:
        """
        Parameters:
        -----------
        event_id : event identificator
        settings : JSONSettings for instance
        addition : more info about an event
        """
        self.send_as_json({'event_id': event_id,
                           'times': times,
                           'addition': addition})
        self.flush()
        self.close()


class TelegramNotifier:
    @staticmethod
    def send_text(text: str, chat_id: Union[str, int], token: str, parse_mode: str = 'Markdown') -> None:
        """
        Send alarmed text message to Telegram bots channel.

        Parameters:
        -----------
        msg : String, message to send

        Returns:
        --------
        rget : requests sending JSON result
        """
        from requests import get as rget

        send_text = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&parse_mode={parse_mode}&text={text}"
        return rget(send_text).json()

    @staticmethod
    def send_photo(photo: str, chat_id: Union[int, str], token: str) -> None:
        from requests import post

        img = open(photo, 'rb')
        url = "https://api.telegram.org/bot{}/sendPhoto".format(token)
        files = {'photo': img}
        data = {'chat_id': chat_id}
        res_json = post(url, files=files, data=data).json()
        img.close()

        return res_json

    @staticmethod
    def send_file(path: str, chat_id: Union[int, str], token: str) -> None:
        from requests import post

        img = open(path, 'rb')
        url = "https://api.telegram.org/bot{}/sendDocument".format(token)
        files = {'document': img}
        data = {'chat_id': chat_id}
        res_json = post(url, files=files, data=data).json()
        img.close()

        return res_json

    @staticmethod
    def send_dataframe_as_html_to_telegram(chat_id: Union[int, str],
                                           token: str, data: pd.DataFrame,
                                           timestamp: str, prefix: str, index: bool = False) -> None:
        """
        Parameters:
        -----------
        data : pandas DataFrame with your data
        timestamp : String formatted datetime, start of event
        prefix : prefix for table name
        """
        f_name = "{}_{}.html".format(prefix, timestamp)
        with open(f_name, "w") as f:
            f.write(data.to_html(index=index))
        TelegramNotifier.send_file(f_name, chat_id, token)


def concat(dataframe_list: list) -> pd.DataFrame:
    """
    Parameters:
    -----------
    dataframe_list : list of pd.DataFrame's, empty is allowed

    Returns:
    --------
    DataFrame : empty DataFrame or concatenated DataFrame
    """
    if len(dataframe_list) > 0:
        return pd.concat(dataframe_list, ignore_index=True)
    return pd.DataFrame()


def deprecated(function: Callable) -> Callable:
    """
    Annotation for deprecated methods.
    """
    def deprecated_wrapper(*args, **kwds):
        logging.warning("You call deprecated function %s!", function.__name__)
        return function(*args, **kwds)
    return deprecated_wrapper


class KafkaLoggingHandler(logging.Handler):
    def __init__(self, kafka_server: str, kafka_topic: str, key: str):
        logging.Handler.__init__(self)
        self.producer = KafkaProducer(value_serializer=lambda m: str(m).encode('ascii'),
                                      key_serializer=lambda m: str(m).encode('ascii'),
                                      bootstrap_servers=kafka_server)
        self.kafka_topic = kafka_topic
        self.key = key

    def emit(self, record):
        #  drop kafka logging to avoid infinite recursion
        if record.name == 'kafka':
            return
        try:
            #  use default formatting
            msg = self.format(record)
            self.producer.send(topic=self.kafka_topic, value=msg, key=self.key)
        except:
            import traceback
            ei = sys.exc_info()
            traceback.print_exception(ei[0], ei[1], ei[2], None, sys.stderr)
            del ei

    def close(self):
        self.producer.flush()
        self.producer.close()
        logging.Handler.close(self)
