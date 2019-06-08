# -*- coding: utf-8 -*-

import logging
import os
import time
import calendar

from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, Unicode, Numeric, String, DateTime, ForeignKey, create_engine
from sqlalchemy import insert, select, and_
from datetime import datetime, timedelta, date

import xlsxwriter


def get_logger(log_level='WARNING', filename=None):
    logger_ = logging.getLogger("MYSCRIPT")

    if logger_.handlers:
        for handler in logger_.handlers[:]:
            handler.close()
            logger_.removeHandler(handler)

    logger_.setLevel(log_level)

    if filename is None:
        filename = os.path.basename(__file__).split(".")[0]

    log_path = os.path.join(r'E:\_job\_logs', filename)
    file_handler = logging.FileHandler(log_path)

    file_handler.setLevel("DEBUG")

    file_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(lineno)-4s <%(funcName)s> - %(name)s: %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    file_handler.setFormatter(file_formatter)
    logger_.addHandler(file_handler)
    return logger_


logger = get_logger(log_level='DEBUG')


DATA_BASE_NAME = "people_quantity_reporter"
DATA_BASE_LOGIN = "postgres"
DATA_BASE_PASS = "123456"


class DatesSupplier:

    @staticmethod
    def get_dates_of_previous_month():
        month_last_day = (datetime.now() - timedelta(datetime.now().day)).date()
        month_first_day = month_last_day - timedelta(month_last_day.day - 1)
        monthdays = [date(month_first_day.year, month_first_day.month, d).isoformat()
                     for d in xrange(1, month_last_day.day+1)]

        return monthdays


    @staticmethod
    def get_dates_of_previous_week():
        week_day = datetime.now().weekday()
        date_monday = (datetime.now() - timedelta(week_day + 7)).date()
        weekdays = [(date_monday + timedelta(i)).isoformat() for i in xrange(0,7)]

        return weekdays


    @staticmethod
    def get_boundary_trassir_timestamp_of_the_day(date_time_obj=None):
        """
        time_yesterday - datatime obj
        return trassir timestamp of begin and end of the time_yesterday
        """
        if date_time_obj is None:
            date_time_obj = datetime.now() - timedelta(1)
        boundary = ['00:00:00', '23:59:59']
        boundary_trassir_timestamp = []
        for x in boundary:
            dt = datetime.strptime("{}-{}-{} {}".format(date_time_obj.timetuple().tm_year,
                                                        date_time_obj.timetuple().tm_mon,
                                                        date_time_obj.timetuple().tm_mday,
                                                        x), "%Y-%m-%d %H:%M:%S")
            _tstmp = "{}000000".format(int(time.mktime(dt.timetuple())))
            boundary_trassir_timestamp.append(_tstmp)

        return boundary_trassir_timestamp[0], boundary_trassir_timestamp[1]


class DataBaseManager:
    def __init__(self):
        self.db_name = DATA_BASE_NAME
        self.db_login = DATA_BASE_LOGIN
        self.db_pass = DATA_BASE_PASS
        self.metadata = MetaData()
        self.retry_counter = 0

        self.people_quantity = self.initialize_people_quantity_table()
        self.connection = self.connect_to_database()
        self.persisting_the_tables()

    def connect_to_database(self):
        self.engine = create_engine(
            'postgresql://{}:{}@localhost:5432/{}'.format(self.db_login,
                                                          self.db_pass,
                                                          self.db_name
                                                          ))
        try:
            return self.engine.connect()
        except Exception as err:
            raise ValueError(unicode(err[0], 'cp1251'))



    def persisting_the_tables(self):
        """
        Создаёт таблицу, если её нет
        """
        self.metadata.create_all(self.engine)

    def initialize_people_quantity_table(self):
        _people_quantity = Table('people quantity detection', self.metadata,
                                     Column('event_id', BigInteger(), primary_key=True, autoincrement=True),
                                     Column('channel_name', Unicode()),
                                     Column('zone_name', Unicode()),
                                     Column('people_quantity', Integer()),
                                     Column('event_ts', BigInteger()),
                                     )
        return _people_quantity

    def insert_data(self, channel_name, zone_name, people_quantity, event_ts):
        ins = self.people_quantity.insert().values(
            channel_name=channel_name,
            zone_name=zone_name,
            people_quantity=people_quantity,
            event_ts=event_ts
        )
        result = self.connection.execute(ins)
        logger.info(result)

    def add_data(self, data):
        """
        Format data before insert to database
        """
        if not isinstance(data, dict):
            return
        event_ts = data.get('event_ts')
        channel_name = data.get('channel_name')
        people_quantity = data.get('people_quantity')
        zone_name = data.get('zone_name')
        if not (event_ts and channel_name and people_quantity and zone_name):
            return
        logger.debug('Start insert data in DB')
        self.insert_data(channel_name, zone_name, people_quantity, event_ts)

    def get_data(self, begin_ts, end_ts):
        """
        makes  querys to database to get information
        """
        #s = select([self.people_quantity]).where(self.people_quantity.c.event_ts.between(begin_ts,end_ts))
        s = select([self.people_quantity]).where(
                and_(self.people_quantity.c.event_ts > begin_ts,
                     self.people_quantity.c.event_ts < end_ts
                     )
                )
        s = s.order_by(self.people_quantity.c.event_ts)
        rp = self.connection.execute(s)
        print(rp.fetchall()[-1])


class Reporter:
    """
    makes xlsx report
    """
    def __init__(self, mail_api, data_loader):
        self._mail_api = mail_api
        self._data_loader = data_loader
        self.report_path = os.getcwd()

    def _send_report(self, filepath):
        """Send report to mail

        Args:
            filepath (str) : Full path to file
        """
        logger.info("Reporter: send file {}".format(filepath))
        self._mail_api.send_file(filepath)

    @staticmethod
    def ts_to_datetime(ts):
        return datetime.fromtimestamp(ts / 10 ** 6)

    def _load_data(self, report_days):
        """Returns data from backup files"""
        data = {day: self._data_loader(day) for day in report_days}
        return data

    def _prepare_report_params(self, today_str):
        """Preparing dates for report"""
        today = datetime.strptime(today_str, '%Y-%m-%d').date()
        yesterday = today - timedelta(days=1)
        report_days = {"daily": [yesterday.isoformat(), today_str]}

        if today.weekday() == 0:
            report_days["weekly"] = self._get_yesterday_week(yesterday)
        if today.day == 1:
            report_days["monthly"] = self._get_yesterday_month(yesterday)

        return report_days, yesterday

    def _make_xlsx(self, report_type, report_data, report_day):
        """Create report in *.xlsx format

        Args:
            report_type (str) : daily/weekly/monthly (used in filename)
            report_data (dict) : All data for report
            report_day (str) : Day for report (used in filename)
        """
        filename = "{}_{}.xlsx".format(report_type, report_day)
        path = os.path.join(self.report_path, filename)
        logger.debug("Reporter: creating {}".format(filename))

        workbook = xlsxwriter.Workbook(path)

        cell_format = {
            "hat": workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'font_size': 14,
            }),
            "header": workbook.add_format({
                'bold': True,
                'align': 'center',
                'valign': 'vcenter',
                'text_wrap': True,
                'border': 1,
            }),
            "text": workbook.add_format({
                'valign': 'vcenter',
                'align': 'center',
                'border': 1,
            }),
            "black": workbook.add_format({
                'font_color': 'black',
            }),
            "grey": workbook.add_format({
                'font_color': '#b2b2b2',
            }),
            "date": workbook.add_format({
                'num_format': 'yyyy.mm.dd',
                'align': 'center',
                'valign': 'vcenter',
                'border': 1,
            }),
            "time": workbook.add_format({
                'num_format': 'hh:mm:ss',
                'align': 'center',
                'valign': 'vcenter',
                'border': 1,
            }),
        }

        for day in sorted(report_data.keys()):
            data = report_data[day]

            worksheet = workbook.add_worksheet(day)
            worksheet.set_column(0, 0, 5)
            worksheet.set_column(1, 2, 12)
            worksheet.set_column(3, 3, 25)
            worksheet.set_column(4, 5, 15)

            if data:
                worksheet.merge_range(0, 0, 0, 5,
                                      'Количество записей в отчете: {}'.format(len(data)),
                                      cell_format["hat"]
                                      )

                worksheet.write(1, 0, "№", cell_format["header"])
                worksheet.write(1, 1, "Дата", cell_format["header"])
                worksheet.write(1, 2, "Время", cell_format["header"])
                worksheet.write(1, 3, "Название камеры", cell_format["header"])
                worksheet.write(1, 4, "Направление", cell_format["header"])
                worksheet.write(1, 5, "Гос. номер", cell_format["header"])
            else:
                worksheet.merge_range(0, 0, 0, 5,
                                      'Днные отсутствуют',
                                      cell_format["hat"])

            for idx, row in enumerate(data, 2):
                dt = self.ts_to_datetime(row['time_enter'])
                plate = self._prepare_plate(
                    row["plate"],
                    cell_format['black'],
                    row["best_guess"],
                    cell_format['grey'],
                    cell_format['text'],
                )

                worksheet.write(idx, 0, idx - 1, cell_format['text'])
                worksheet.write(idx, 1, dt, cell_format['date'])
                worksheet.write(idx, 2, dt, cell_format['time'])
                worksheet.write(idx, 3, row["channel_name"], cell_format['text'])
                worksheet.write(idx, 4, row["direction"], cell_format['text'])
                worksheet.write_rich_string(idx, 5, *plate)

        workbook.close()

        return path

    def generate_reports(self, today_str):
        """Generate reports for yesterday

        Args:
            today_str (str) : Today date in isoformat "%Y-%m-%d"
            report_days = {'monthly': ['2019-04-01',  ..., '2019-04-30'],
                           'weekly': ['2019-04-22', ..., '2019-04-28'],
                           'daily': ['2019-04-30']}
        """
        try:
            report_days, yesterday = self._prepare_report_params(today_str)
            report_data = {report_type: self._load_data(report_dates)
                           for report_type, report_dates in report_days.iteritems()}

            reports = [self._make_xlsx(report_type, report_data, yesterday.isoformat())
                       for report_type, report_data in report_data.iteritems()]

            for report in reports:
                self._send_report(report)
        except:
            """Catch exceptions in thread"""
            logger.critical("Generate reports error", exc_info=True)

class Manager:
    def __init__(self):
        pass

    def is_it_the_next_day(self):
        pass

    def loop_runner(self):
        pass



print(DatesSupplier.get_boundary_trassir_timestamp_of_previous_month())
db_manager = DataBaseManager()
#db_manager.get_data(start, end)