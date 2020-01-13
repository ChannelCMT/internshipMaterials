import talib as ta
import numpy as np
import pandas as pd

"""
将kdj策略需要用到的信号生成器抽离出来
"""

class rBreakSignal():

    def __init__(self):
        self.author = 'channel'
    
    def rBreak(self, am, paraDict):
        observedPct = paraDict['observedPct']
        reversedPct = paraDict['reversedPct']
        breakPct = paraDict['breakPct']
        rangePeriod = paraDict['rangePeriod']

        pastHigh = ta.MAX(am.high, rangePeriod)
        pastLow = ta.MIN(am.low, rangePeriod)

        observedLong = pastLow-observedPct*(pastHigh-am.close)
        observedShort = pastHigh+observedPct*(am.close-pastLow)
        reversedLong = ((1-reversedPct)*pastHigh + (1+reversedPct)*pastLow)/2
        reversedShort = ((1-reversedPct)*pastLow + (1+reversedPct)*pastHigh)/2
        breakLong = observedShort+breakPct*(observedShort-observedLong)
        breakShort = observedLong-breakPct*(observedShort-observedLong)
        return observedLong, observedShort, reversedLong, reversedShort, breakLong, breakShort

    def fliterVol(self, am, paraDict):
        volPeriod = paraDict['volPeriod']
        lowVolThreshold = paraDict['lowVolThreshold']

        std = ta.STDDEV(am.close, volPeriod)
        atr = ta.ATR(am.high, am.low, am.close, volPeriod)
        rangeHL = ta.MAX(am.high, volPeriod)-ta.MIN(am.low, volPeriod)
        minVol = min(std[-1], atr[-1], rangeHL[-1])
        lowFilterRange = am.close[-1]*lowVolThreshold
        filterCanTrade = 1 if (minVol >= lowFilterRange) else -1
        return filterCanTrade



        