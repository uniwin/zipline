#
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import math

from logbook import Logger
from collections import defaultdict

import pandas as pd
from six import iteritems, itervalues

from zipline.finance.order import Order

from zipline.finance.slippage import VolumeShareSlippage
from zipline.finance.commission import PerShare
from zipline.finance.cancel_policy import NeverCancel

from zipline.utils.serialization_utils import (
    VERSION_LABEL
)

log = Logger('Blotter')


class Blotter(object):
    def __init__(self, data_frequency, asset_finder,
                 slippage_func=None, commission=None):
        # these orders are aggregated by sid
        self.open_orders = defaultdict(list)

        # keep a dict of orders by their own id
        self.orders = {}

        # all our legacy order management code works with integer sids.
        # this lets us convert those to assets when needed.  ideally, we'd just
        # revamp all the legacy code to work with assets.
        self.asset_finder = asset_finder

        # holding orders that have come in since the last event.
        self.new_orders = []
        self.current_dt = None

        self.max_shares = int(1e+11)

        self.slippage_func = slippage_func or VolumeShareSlippage()
        self.commission = commission or PerShare()

        self.data_frequency = data_frequency

    def __repr__(self):
        return """
{class_name}(
    slippage={slippage_func},
    commission={commission},
    open_orders={open_orders},
    orders={orders},
    new_orders={new_orders},
    current_dt={current_dt})
""".strip().format(class_name=self.__class__.__name__,
                   slippage_func=self.slippage_func,
                   commission=self.commission,
                   open_orders=self.open_orders,
                   orders=self.orders,
                   new_orders=self.new_orders,
                   current_dt=self.current_dt)

    def set_date(self, dt):
        self.current_dt = dt

    def order(self, sid, amount, style, cancel_policy=NeverCancel(),
              order_id=None):

        # something could be done with amount to further divide
        # between buy by share count OR buy shares up to a dollar amount
        # numeric == share count  AND  "$dollar.cents" == cost amount

        """
        amount > 0 :: Buy/Cover
        amount < 0 :: Sell/Short
        Market order:    order(sid, amount)
        Limit order:     order(sid, amount, style=LimitOrder(limit_price))
        Stop order:      order(sid, amount, style=StopOrder(stop_price))
        StopLimit order: order(sid, amount, style=StopLimitOrder(limit_price,
                               stop_price))
        """
        if amount == 0:
            # Don't bother placing orders for 0 shares.
            return
        elif amount > self.max_shares:
            # Arbitrary limit of 100 billion (US) shares will never be
            # exceeded except by a buggy algorithm.
            raise OverflowError("Can't order more than %d shares" %
                                self.max_shares)

        is_buy = (amount > 0)
        order = Order(
            dt=self.current_dt,
            sid=sid,
            amount=amount,
            cancel_policy=cancel_policy,
            stop=style.get_stop_price(is_buy),
            limit=style.get_limit_price(is_buy),
            id=order_id
        )

        self.open_orders[order.sid].append(order)
        self.orders[order.id] = order
        self.new_orders.append(order)

        return order.id

    def cancel(self, order_id):
        if order_id not in self.orders:
            return

        cur_order = self.orders[order_id]

        if cur_order.open:
            order_list = self.open_orders[cur_order.sid]
            if cur_order in order_list:
                order_list.remove(cur_order)

            if cur_order in self.new_orders:
                self.new_orders.remove(cur_order)
            cur_order.cancel()
            cur_order.dt = self.current_dt
            # we want this order's new status to be relayed out
            # along with newly placed orders.
            self.new_orders.append(cur_order)

    def execute_cancel_policy(self, dt, event):
        for orders_by_sid in itervalues(self.open_orders):
            for order in orders_by_sid:
                if order.should_cancel(dt, event):
                    self.cancel(order.id)

    def reject(self, order_id, reason=''):
        """
        Mark the given order as 'rejected', which is functionally similar to
        cancelled. The distinction is that rejections are involuntary (and
        usually include a message from a broker indicating why the order was
        rejected) while cancels are typically user-driven.
        """
        if order_id not in self.orders:
            return

        cur_order = self.orders[order_id]

        order_list = self.open_orders[cur_order.sid]
        if cur_order in order_list:
            order_list.remove(cur_order)

        if cur_order in self.new_orders:
            self.new_orders.remove(cur_order)
        cur_order.reject(reason=reason)
        cur_order.dt = self.current_dt
        # we want this order's new status to be relayed out
        # along with newly placed orders.
        self.new_orders.append(cur_order)

    def hold(self, order_id, reason=''):
        """
        Mark the order with order_id as 'held'. Held is functionally similar
        to 'open'. When a fill (full or partial) arrives, the status
        will automatically change back to open/filled as necessary.
        """
        if order_id not in self.orders:
            return

        cur_order = self.orders[order_id]
        if cur_order.open:
            if cur_order in self.new_orders:
                self.new_orders.remove(cur_order)
            cur_order.hold(reason=reason)
            cur_order.dt = self.current_dt
            # we want this order's new status to be relayed out
            # along with newly placed orders.
            self.new_orders.append(cur_order)

    def process_splits(self, splits):
        """
        Processes a list of splits by modifying any open orders as needed.

        Parameters
        ----------
        splits: list
            A list of splits.  Each split is a tuple of (sid, ratio).

        Returns
        -------
        None
        """
        for split in splits:
            sid = split[0]
            if sid not in self.open_orders:
                return

            orders_to_modify = self.open_orders[sid]
            for order in orders_to_modify:
                order.handle_split(split[1])

    def get_transactions(self, bar_data):
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Parameters
        ----------
        bar_data: zipline._protocol.BarData

        Notes
        -----
        This method book-keeps the blotter's open_orders dictionary, so that
         it is accurate by the time we're done processing open orders.

        Returns
        -------
        transactions_list: List
            transactions_list: list of transactions resulting from the current
            open orders.  If there were no open orders, an empty list is
            returned.

        commissions_list: List
            commissions_list: list of commissions resulting from filling the
            open orders.  A commission is an object with "sid" and "cost"
            parameters.  If there are no commission events (because, for
            example, Zipline models the commission cost into the fill price
            of the transaction), then this is None.
        """
        closed_orders = []
        transactions = []

        assets = self.asset_finder.retrieve_all(self.open_orders.keys())
        asset_dict = {asset.sid: asset for asset in assets}

        for sid, asset_orders in iteritems(self.open_orders):
            asset = asset_dict[sid]

            for order, txn in \
                    self.slippage_func(bar_data, asset, asset_orders):
                direction = math.copysign(1, txn.amount)
                per_share, total_commission = self.commission.calculate(txn)
                txn.price += per_share * direction
                txn.commission = total_commission
                order.filled += txn.amount

                if txn.commission is not None:
                    order.commission = (order.commission or 0.0) + \
                        txn.commission

                txn.dt = pd.Timestamp(txn.dt, tz='UTC')
                order.dt = txn.dt

                transactions.append(txn)

                if not order.open:
                    closed_orders.append(order)

        # remove all closed orders from our open_orders dict
        for order in closed_orders:
            sid = order.sid
            try:
                sid_orders = self.open_orders[sid]
                sid_orders.remove(order)
            except KeyError:
                continue

        # now clear out the sids from our open_orders dict that have
        # zero open orders
        for sid in list(self.open_orders.keys()):
            if len(self.open_orders[sid]) == 0:
                del self.open_orders[sid]

        # FIXME this API doesn't feel right (returning two things here)
        return transactions, None

    def __getstate__(self):

        state_to_save = ['new_orders', 'orders', '_status', 'data_frequency']

        state_dict = {k: self.__dict__[k] for k in state_to_save
                      if k in self.__dict__}

        # Have to handle defaultdicts specially
        state_dict['open_orders'] = dict(self.open_orders)

        STATE_VERSION = 1
        state_dict[VERSION_LABEL] = STATE_VERSION

        return state_dict

    def __setstate__(self, state):
        # FIXME this doesn't work (passing in None assetfinder as second
        # param), but srz/desrz code isn't used in prod. can fix later.
        self.__init__(state.pop('data_frequency'), None)

        OLDEST_SUPPORTED_STATE = 1
        version = state.pop(VERSION_LABEL)

        if version < OLDEST_SUPPORTED_STATE:
            raise BaseException("Blotter saved is state too old.")

        open_orders = defaultdict(list)
        open_orders.update(state.pop('open_orders'))
        self.open_orders = open_orders

        self.__dict__.update(state)
