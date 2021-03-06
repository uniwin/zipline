"""
filter.py
"""
from numpy import (
    float64,
    nan,
    nanpercentile,
)
from itertools import chain
from operator import attrgetter

from zipline.errors import (
    BadPercentileBounds,
    UnsupportedDataType,
)
from zipline.lib.rank import ismissing
from zipline.pipeline.mixins import (
    CustomTermMixin,
    PositiveWindowLengthMixin,
    SingleInputMixin,
)
from zipline.pipeline.term import ComputableTerm
from zipline.pipeline.expression import (
    BadBinaryOperator,
    FILTER_BINOPS,
    method_name_for_op,
    NumericalExpression,
)
from zipline.utils.control_flow import nullctx
from zipline.utils.numpy_utils import bool_dtype


def concat_tuples(*tuples):
    """
    Concatenate a sequence of tuples into one tuple.
    """
    return tuple(chain(*tuples))


def binary_operator(op):
    """
    Factory function for making binary operator methods on a Filter subclass.

    Returns a function "binary_operator" suitable for implementing functions
    like __and__ or __or__.
    """
    # When combining a Filter with a NumericalExpression, we use this
    # attrgetter instance to defer to the commuted interpretation of the
    # NumericalExpression operator.
    commuted_method_getter = attrgetter(method_name_for_op(op, commute=True))

    def binary_operator(self, other):
        if isinstance(self, NumericalExpression):
            self_expr, other_expr, new_inputs = self.build_binary_op(
                op, other,
            )
            return NumExprFilter.create(
                "({left}) {op} ({right})".format(
                    left=self_expr,
                    op=op,
                    right=other_expr,
                ),
                new_inputs,
            )
        elif isinstance(other, NumericalExpression):
            # NumericalExpression overrides numerical ops to correctly handle
            # merging of inputs.  Look up and call the appropriate
            # right-binding operator with ourself as the input.
            return commuted_method_getter(other)(self)
        elif isinstance(other, Filter):
            if self is other:
                return NumExprFilter.create(
                    "x_0 {op} x_0".format(op=op),
                    (self,),
                )
            return NumExprFilter.create(
                "x_0 {op} x_1".format(op=op),
                (self, other),
            )
        elif isinstance(other, int):  # Note that this is true for bool as well
            return NumExprFilter.create(
                "x_0 {op} ({constant})".format(op=op, constant=int(other)),
                binds=(self,),
            )
        raise BadBinaryOperator(op, self, other)

    binary_operator.__doc__ = "Binary Operator: '%s'" % op
    return binary_operator


def unary_operator(op):
    """
    Factory function for making unary operator methods for Filters.
    """
    valid_ops = {'~'}
    if op not in valid_ops:
        raise ValueError("Invalid unary operator %s." % op)

    def unary_operator(self):
        # This can't be hoisted up a scope because the types returned by
        # unary_op_return_type aren't defined when the top-level function is
        # invoked.
        if isinstance(self, NumericalExpression):
            return NumExprFilter.create(
                "{op}({expr})".format(op=op, expr=self._expr),
                self.inputs,
            )
        else:
            return NumExprFilter.create("{op}x_0".format(op=op), (self,))

    unary_operator.__doc__ = "Unary Operator: '%s'" % op
    return unary_operator


class Filter(ComputableTerm):
    """
    Pipeline API expression producing boolean-valued outputs.
    """
    dtype = bool_dtype

    clsdict = locals()
    clsdict.update(
        {
            method_name_for_op(op): binary_operator(op)
            for op in FILTER_BINOPS
        }
    )
    clsdict.update(
        {
            method_name_for_op(op, commute=True): binary_operator(op)
            for op in FILTER_BINOPS
        }
    )

    __invert__ = unary_operator('~')

    def _validate(self):
        # Run superclass validation first so that we handle `dtype not passed`
        # before this.
        retval = super(Filter, self)._validate()
        if self.dtype != bool_dtype:
            raise UnsupportedDataType(
                typename=type(self).__name__,
                dtype=self.dtype
            )
        return retval


class NumExprFilter(NumericalExpression, Filter):
    """
    A Filter computed from a numexpr expression.
    """

    @classmethod
    def create(cls, expr, binds):
        """
        Helper for creating new NumExprFactors.

        This is just a wrapper around NumericalExpression.__new__ that always
        forwards `bool` as the dtype, since Filters can only be of boolean
        dtype.
        """
        return cls(expr=expr, binds=binds, dtype=bool_dtype)

    def _compute(self, arrays, dates, assets, mask):
        """
        Compute our result with numexpr, then re-apply `mask`.
        """
        return super(NumExprFilter, self)._compute(
            arrays,
            dates,
            assets,
            mask,
        ) & mask


class NullFilter(SingleInputMixin, Filter):
    """
    A Filter indicating whether input values are missing from an input.

    Parameters
    ----------
    factor : zipline.pipeline.factor.Factor
        The factor to compare against its missing_value.
    """
    window_length = 0

    def __new__(cls, factor):
        return super(NullFilter, cls).__new__(
            cls,
            inputs=(factor,),
        )

    def _compute(self, arrays, dates, assets, mask):
        return ismissing(arrays[0], self.inputs[0].missing_value)


class PercentileFilter(SingleInputMixin, Filter):
    """
    A Filter representing assets falling between percentile bounds of a Factor.

    Parameters
    ----------
    factor : zipline.pipeline.factor.Factor
        The factor over which to compute percentile bounds.
    min_percentile : float [0.0, 1.0]
        The minimum percentile rank of an asset that will pass the filter.
    max_percentile : float [0.0, 1.0]
        The maxiumum percentile rank of an asset that will pass the filter.
    """
    window_length = 0

    def __new__(cls, factor, min_percentile, max_percentile, mask):
        return super(PercentileFilter, cls).__new__(
            cls,
            inputs=(factor,),
            mask=mask,
            min_percentile=min_percentile,
            max_percentile=max_percentile,
        )

    def _init(self, min_percentile, max_percentile, *args, **kwargs):
        self._min_percentile = min_percentile
        self._max_percentile = max_percentile
        return super(PercentileFilter, self)._init(*args, **kwargs)

    @classmethod
    def static_identity(cls, min_percentile, max_percentile, *args, **kwargs):
        return (
            super(PercentileFilter, cls).static_identity(*args, **kwargs),
            min_percentile,
            max_percentile,
        )

    def _validate(self):
        """
        Ensure that our percentile bounds are well-formed.
        """
        if not 0.0 <= self._min_percentile < self._max_percentile <= 100.0:
            raise BadPercentileBounds(
                min_percentile=self._min_percentile,
                max_percentile=self._max_percentile,
            )
        return super(PercentileFilter, self)._validate()

    def _compute(self, arrays, dates, assets, mask):
        """
        For each row in the input, compute a mask of all values falling between
        the given percentiles.
        """
        # TODO: Review whether there's a better way of handling small numbers
        # of columns.
        data = arrays[0].copy().astype(float64)
        data[~mask] = nan

        # FIXME: np.nanpercentile **should** support computing multiple bounds
        # at once, but there's a bug in the logic for multiple bounds in numpy
        # 1.9.2.  It will be fixed in 1.10.
        # c.f. https://github.com/numpy/numpy/pull/5981
        lower_bounds = nanpercentile(
            data,
            self._min_percentile,
            axis=1,
            keepdims=True,
        )
        upper_bounds = nanpercentile(
            data,
            self._max_percentile,
            axis=1,
            keepdims=True,
        )
        return (lower_bounds <= data) & (data <= upper_bounds)


class CustomFilter(PositiveWindowLengthMixin, CustomTermMixin, Filter):
    """
    Base class for user-defined Filters.

    Parameters
    ----------
    inputs : iterable, optional
        An iterable of `BoundColumn` instances (e.g. USEquityPricing.close),
        describing the data to load and pass to `self.compute`.  If this
        argument is passed to the CustomFilter constructor, we look for a
        class-level attribute named `inputs`.
    window_length : int, optional
        Number of rows to pass for each input.  If this argument is not passed
        to the CustomFilter constructor, we look for a class-level attribute
        named `window_length`.

    Notes
    -----
    Users implementing their own Filters should subclass CustomFilter and
    implement a method named `compute` with the following signature:

    .. code-block:: python

        def compute(self, today, assets, out, *inputs):
           ...

    On each simulation date, ``compute`` will be called with the current date,
    an array of sids, an output array, and an input array for each expression
    passed as inputs to the CustomFilter constructor.

    The specific types of the values passed to `compute` are as follows::

        today : np.datetime64[ns]
            Row label for the last row of all arrays passed as `inputs`.
        assets : np.array[int64, ndim=1]
            Column labels for `out` and`inputs`.
        out : np.array[bool, ndim=1]
            Output array of the same shape as `assets`.  `compute` should write
            its desired return values into `out`.
        *inputs : tuple of np.array
            Raw data arrays corresponding to the values of `self.inputs`.

    See the documentation for
    :class:`~zipline.pipeline.factors.factor.CustomFactor` for more details on
    implementing a custom ``compute`` method.

    See Also
    --------
    zipline.pipeline.factors.factor.CustomFactor
    """
    ctx = nullctx()
