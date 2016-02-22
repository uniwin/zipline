# Copyright 2016 Quantopian, Inc.
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

from numpy import dtype

from zipline.pipeline.data.equity_pricing import USEquityPricing
from zipline.lib._float64window import AdjustedArrayWindow as Float64Window
from zipline.lib._int64window import AdjustedArrayWindow as Int64Window
from zipline.lib.adjustment import Float64Multiply

##

#        return self._iterator_type(
#            data, np.array
#            self._viewtype, dtype('float64')
#            self.adjustments, Float64Multiply(first_row=0, last_row=0, first_col=0, last_col=0)
#            offset, int 
#            window_length, windowlength
#        )

class Block(object):

    def __init__(self, window, cal_start, cal_end):
        self.window = window
        self.cal_start = cal_start
        self.cal_end = cal_end
        self.current = next(window)

    def get(self, end_ix):
        # TODO: Get this working and boundary condition.
        anchor = end_ix - self.cal_start
        while self.window.anchor < anchor:
            self.current = next(self.window)
        return self.current[:, 0]


class USEquityHistoryLoader(object):

    def __init__(self, daily_reader, adjustment_reader):
        self._daily_reader = daily_reader
        self._calendar = daily_reader._calendar
        self._adjustments_reader = adjustment_reader

        self._daily_window_blocks = {}

    def _get_adjustments_in_range(self, asset, days, field):
        sid = int(asset)
        start = days[0]
        end = days[-1]
        adjs = {}
        if field != 'volume':
            mergers = self._adjustments_reader.get_adjustments_for_sid(
                'mergers', sid)
            for m in mergers:
                dt = m[0]
                if start < dt <= end:
                    end_loc = max(days.get_loc(dt) - 1, 0)
                    adjs[end_loc] = [Float64Multiply(0,
                                                     end_loc,
                                                     0,
                                                     0,
                                                     m[1])]
            divs = self._adjustments_reader.get_adjustments_for_sid(
                'dividends', sid)
            if field != 'volume':
                for d in divs:
                    dt = d[0]
                    if start < dt <= end:
                        end_loc = days.get_loc(dt)
                        adjs[end_loc] = [Float64Multiply(0,
                                                         end_loc,
                                                         0,
                                                         0,
                                                         d[1])]
        splits = self._adjustments_reader.get_adjustments_for_sid(
            'splits', sid)
        for s in splits:
            dt = s[0]
            if field == 'volume':
                ratio = s[1] / 1.0
            else:
                ratio = s[1]
            if start < dt <= end:
                end_loc = max(days.get_loc(dt) - 1, 0)
                adjs[end_loc] = [Float64Multiply(0,
                                                 end_loc,
                                                 0,
                                                 0,
                                                 ratio)]
        print days
        print adjs
        return adjs

    def _ensure_block(self, asset, start, end, size, start_ix, end_ix, field):
        try:
            block = self._daily_window_blocks[(asset, field, size)]
            if start_ix >= block.cal_start and end_ix <= block.cal_end:
                return block
        except KeyError:
            pass

        col = getattr(USEquityPricing, field)
        cal = self._calendar
        prefetch_end_ix = min(end_ix + 40, len(cal) - 1)
        prefetch_end = cal[prefetch_end_ix]
        array = self._daily_reader.load_raw_arrays(
            [col], start, prefetch_end, [asset])[0]
        days = cal[start_ix:prefetch_end_ix]
        if self._adjustments_reader:
            adjs = self._get_adjustments_in_range(asset, days, col)
        else:
            adjs = {}
        if field == 'volume':
            window_type = Int64Window
            dtype_ = dtype('int64')
            array = array.astype('int64')
        else:
            window_type = Float64Window
            dtype_ = dtype('float64')

        window = window_type(
            array,
            dtype_,
            adjs,
            0,
            size
        )
        block = Block(window, start_ix, prefetch_end_ix)
        self._daily_window_blocks[(asset, field, size)] = block
        return block

    def history(self, asset, start, end, size, field):
        start_ix = self._calendar.get_loc(start)
        end_ix = self._calendar.get_loc(end)
        block = self._ensure_block(
            asset, start, end, size, start_ix, end_ix, field)
        # TODO: get most recent value from Window
        return block.get(end_ix)
