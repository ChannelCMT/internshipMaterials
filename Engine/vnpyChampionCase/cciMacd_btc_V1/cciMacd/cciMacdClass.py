import talib as ta
import numpy as np
import pandas as pd

"""
将kdj策略需要用到的信号生成器抽离出来
"""

class cciMacdSignal():

    def __init__(self):
        self.strategyName = 'cciMacd'

    def macdStatus(self, am, paraDict):
        fastPeriod = paraDict["fastPeriod"]
        slowPeriod = paraDict["slowPeriod"]
        sigPeriod = paraDict["sigPeriod"]

        macd, macdSig, _ = ta.MACD(am.close, fastPeriod, slowPeriod, sigPeriod)
        momDirection = 1 if macd[-1]>macdSig[-1] else -1
        
        return momDirection, macd, macdSig

    def cciEntrySignal(self, am, paraDict):
        cciPeriod = paraDict["cciPeriod"]
        cciThreshold = paraDict["cciEntryThreshold"]
        cci = ta.CCI(am.high, am.low, am.close, cciPeriod)
        cciMax = ta.MAX(cci, 3)
        cciMin = ta.MIN(cci, 3)

        cciSig = 0
        if cci[-1]>cciThreshold:
            cciSig = 1
        elif cci[-1]<-cciThreshold:
            cciSig = -1
        return cciSig, cci
    
    def exitSignal(self, am, paraDict):
        cciPeriod = paraDict["cciPeriod"]
        cciExitThreshold = paraDict["cciExitThreshold"]
        fastPeriod = paraDict["fastPeriod"]
        slowPeriod = paraDict["slowPeriod"]
        sigPeriod = paraDict["sigPeriod"]

        cci = ta.CCI(am.high, am.low, am.close, cciPeriod)
        macd, macdSig, _ = ta.MACD(am.close, fastPeriod, slowPeriod, sigPeriod)
        momDirection = 1 if macd[-1]>macdSig[-1] else -1

        exitLong, exitShort = 0, 0
        if cci[-1]<cciExitThreshold or momDirection<0:
            exitLong = 1
        if cci[-1]>-cciExitThreshold or momDirection>0:
            exitShort = 1
        return exitLong, exitShort

    def fliterVol(self, am, paraDict):
        # 过滤低波动与低成交量
        volPeriod = paraDict['volPeriod']
        lowVolThreshold = paraDict['lowVolThreshold']
        highVolThreshold = paraDict['highVolThreshold']

        atr = ta.ATR(am.high, am.low, am.close, volPeriod)
        _, _, volumeLower = ta.BBANDS(am.volume, volPeriod, 1, 1)

        lowFilterRange = am.close[-1]*lowVolThreshold
        highFilterRange = am.close[-1]*highVolThreshold

        filterCanTrade = 1 if (atr[-1] >= lowFilterRange) else 0
        lowVolume = 1 if (am.volume[-1] <= volumeLower[-1]) else 0
        highVolStatus = 1 if (atr[-1] >= highFilterRange) else 0
        minusPos = 0.62**(lowVolume+highVolStatus)
        return filterCanTrade, minusPos

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