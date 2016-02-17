from cython cimport boundscheck
from numpy cimport float64_t, int64_t, intp_t
from numpy import searchsorted

@boundscheck(False)
cpdef _apply_adjustments_to_window(list adjustments_list,
                                   float64_t[:] window_data,
                                   int64_t[:]dts_in_window,
                                   multiply):
    if len(adjustments_list) == 0:
        return

    # advance idx to the correct spot in the adjustments list, based on
    # when the window starts
    idx = 0

    while idx < len(adjustments_list) and dts_in_window[0] >\
          adjustments_list[idx][0].value:
        idx += 1

    # if we've advanced through all the adjustments, then there's nothing
    # to do.
    if idx == len(adjustments_list):
        return

    cdef float64_t adj, inverse
    cdef intp_t i

    while idx < len(adjustments_list):
        adjustment_to_apply = adjustments_list[idx]
        adj_dt = adjustment_to_apply[0].value
        if adj_dt > dts_in_window[-1]:
            break

        range_end = searchsorted(dts_in_window, adj_dt)
        if multiply:
            adj = adjustment_to_apply[1]
            for i in range(range_end):
                window_data[i] *= adj
        else:
            inverse = 1.0 / adjustment_to_apply[1]
            for i in range(range_end):
                window_data[i] *= inverse
            
        idx += 1
