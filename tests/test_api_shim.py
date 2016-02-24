from unittest import TestCase
import pandas as pd
from logbook import TestHandler, WARNING
from nose.tools import nottest
from testfixtures import TempDirectory

from zipline import TradingAlgorithm
from zipline.data.minute_bars import BcolzMinuteBarWriter, \
    US_EQUITIES_MINUTES_PER_DAY
from zipline.finance.trading import TradingEnvironment, SimulationParameters
from zipline.utils.test_utils import write_minute_data_for_asset, FakeDataPortal

specific_sid_algo = """
def initialize(context):
    context.sid1 = sid(1)
    context.sid2 = sid(2)

def handle_data(context, data):
    assert sid(1) in data
    assert sid(2) in data
"""

order_algo = """
from zipline.api import sid, order
def initialize(context):
    context.count = 0

def handle_data(context, data):
    if context.count == 0:
        order(sid(1), 1)
    elif context.count == 4:
        assert sid(1) in data
        assert sid(2) not in data
    elif context.count == 6:
        assert sid(1) in data
        assert sid(2) not in data
        order(sid(2), 1)
        assert sid(1) in data
        assert sid(2) in data
    elif context.count == 10:
        assert sid(1) in data
        assert sid(2) in data

"""


class TestAPIShim(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env = TradingEnvironment()
        cls.tempdir = TempDirectory()

        cls.trading_days = cls.env.days_in_range(
            start=pd.Timestamp("2016-01-05", tz='UTC'),
            end=pd.Timestamp("2016-01-07", tz='UTC')
        )

        equities_data = {}
        for sid in [1, 2]:
            equities_data[sid] = {
                    "start_date": cls.trading_days[0],
                    "end_date": cls.env.next_trading_day(cls.trading_days[-1]),
                    "symbol": "ASSET{0}".format(sid),
            }

        cls.env.write_data(equities_data=equities_data)

        cls.asset1 = cls.env.asset_finder.retrieve_asset(1)
        cls.asset2 = cls.env.asset_finder.retrieve_asset(2)

        market_opens = cls.env.open_and_closes.market_open.loc[
            cls.trading_days]

        writer = BcolzMinuteBarWriter(
            cls.trading_days[0],
            cls.tempdir.path,
            market_opens,
            US_EQUITIES_MINUTES_PER_DAY
        )

        write_minute_data_for_asset(
            cls.env, writer, cls.trading_days[0], cls.trading_days[-1], 1,
        )

        cls.sim_params = SimulationParameters(
            period_start=cls.trading_days[0],
            period_end=cls.trading_days[-1],
            data_frequency="minute",
            env=cls.env
        )

    @classmethod
    def tearDownClass(cls):
        cls.tempdir.cleanup()

    @classmethod
    def create_algo(cls, code):
        return TradingAlgorithm(
            script=code,
            sim_params=cls.sim_params,
            env=cls.env
        )

    # FIXME ZIPLINE CAN ONLY TEST ORDERS + POSITIONS!
    def test_specific_assets(self):
        algo = self.create_algo(specific_sid_algo)
        handler = make_test_handler(self)

        with handler.applicationbound():
            gen = algo.get_generator()
            gen.next()

        warnings = [record for record in handler.records if
                    record.level == WARNING]

        # should be 780 warnings from just one day
        self.assertEqual(780, len(warnings))

        for idx, warning in enumerate(warnings):
            if idx % 2 == 0:
                self.assertEqual("<string>:7: ZiplineDeprecationWarning: "
                                 "Checking whether an asset is in data is "
                                 "deprecated.",
                                 warning.message)
            else:
                self.assertEqual("<string>:8: ZiplineDeprecationWarning: "
                                 "Checking whether an asset is in data is "
                                 "deprecated.",
                                 warning.message)

        self.assertIn(self.asset1, algo.spe._specific_assets)
        pass

    def test_data_warning(self):
        algo = self.create_algo(order_algo)
        handler = make_test_handler(self)

        with handler.applicationbound():
            results = algo.run(FakeDataPortal(self.env))

@nottest
def make_test_handler(testcase, *args, **kwargs):
    handler = TestHandler(*args, **kwargs)
    testcase.addCleanup(handler.close)
    return handler


