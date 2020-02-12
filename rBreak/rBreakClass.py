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
        lowVolThreshold = paraDict['lowVolThreshold']

        _, _, _, _, breakLong, breakShort = self.rBreak(am, paraDict)
        hlPct = (breakLong - breakShort)/am.close
        filterCanTrade = 1 if hlPct[-1]>lowVolThreshold else -1
        return filterCanTrade
    
    def bigVolWeight(self, am, paraDict):
        bigVolPeriod = paraDict['bigVolPeriod']
        bigVolThreshold = paraDict['bigVolThreshold']
        bigMultiplier = paraDict['bigMultiplier']

        rangeHL = ta.MAX(am.high, bigVolPeriod)-ta.MIN(am.low, bigVolPeriod)
        bigVolRange = am.close[-1]*bigVolThreshold
        bigVol = 1 if (rangeHL[-1] >= bigVolRange) else 0

        lotMultiplier = 1
        if bigVol:
            lotMultiplier = bigMultiplier
        return lotMultiplier


        