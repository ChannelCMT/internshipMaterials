from vnpy.trader.vtConstant import *
import numpy as np
import talib as ta
import pandas as pd
from datetime import timedelta, datetime
from vnpy.trader.utils.templates.orderTemplate import * 
from vnpy.trader.app.ctaStrategy import ctaBase
from cciMacdClass import cciMacdSignal

########################################################################
class cciMacdStrategy(OrderTemplate):
    className = 'cciMacd'

    # 参数列表，保存了参数的名称
    paramList = [
                 'author',
                 # 进场手数
                 'totalCapital', 'palValue',
                 "posWeight", "pctPerTrade",  "maxPctPos",
                 # 品种列表
                 'symbolList', 'barPeriod',
                 # signalParameter 计算信号的参数
                 'fastPeriod', 'slowPeriod',
                 'sigPeriod','cciPeriod',
                 # 出场后停止的小时
                 'cciThreshold',
                 # 波动率过滤阈值
                 'volPeriod', 'lowVolthreshold','highVolThreshold',
                 'stoplossPct',
                 # 时间周期
                 'timeframeMap',
                 #  总秒，间隔，下单次数
                 'totalSecond', 'stepSecond','orderTime'
                 ]

    # 变量列表，保存了变量的名称
    varList = [
                'nPos'
               ]
    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['posDict', 'eveningDict']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        super().__init__(ctaEngine, setting)
        self.paraDict = setting
        self.symbol = self.symbolList[0]
        self.algorithm = cciMacdSignal()
        self.lot = 0

        # varialbes
        self.orderDict = {
                         'orderLongSet1':set(), 'orderShortSet1':set(), 
                         }
        
        self.orderLastList = []
        # self.lastOrderDict = {'nextExecuteTime': datetime(2000, 1, 1)}
        self.nPos = 0

        # 打印全局信号的字典
        self.globalStatus = {}

        self.chartLog = {
                'datetime':[],
                'macd':[],
                'macdSig':[],
                'cci':[]
                }

    def prepare_data(self):
        for timeframe in list(set(self.timeframeMap.values())):
            self.registerOnBar(self.symbol, timeframe, None)

    def arrayPrepared(self, period):
        am = self.getArrayManager(self.symbol, period)
        if not am.inited:
            return False, None
        else:
            return True, am

    # ----------------------------------------------------------------------
    def onInit(self):
        self.setArrayManagerSize(self.barPeriod)
        self.prepare_data()
        self.mail("chushihuaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStart(self):
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        self.putEvent()

    # 定时清除已经出场的单
    def delOrderID(self, orderSet):
        for orderId in list(orderSet):
            op = self._orderPacks[orderId]
            # 检查是否完全平仓
            if self.orderClosed(op):
                # 在记录中删除
                orderSet.discard(orderId)
    
    # 获得执行价格
    def priceExecute(self, bar):
        if bar.vtSymbol in self._tickInstance:
            tick = self._tickInstance[bar.vtSymbol]
            if tick.datetime >= bar.datetime:
                return tick.upperLimit * 0.99, tick.lowerLimit*1.01
        return bar.close*1.02, bar.close*0.98

    # 获取当前的持有仓位
    def getHoldVolume(self, orderSet):
        pos = 0
        for orderID in orderSet:
            op = self._orderPacks[orderID]
            holdVolume = op.order.tradedVolume
            pos+= holdVolume
        return pos

    def calCurrentLot(self, bar):
        engineType = self.getEngineType()  # 判断engine模式
        if engineType != 'backtesting':  # 回测时下单手数设置的是一次下多少个币，实盘需要根据情况转化为合约张数
            # 实盘下单手数按此方法调整
            # 可用资金量: self.totalCapital * self.posWeight, 所有和下单，持仓相关的参数都是基于这个的一个比例，实盘的时候只需要修改posWeight来调整策略权重
            self.totalCapital = self.accountDict.get(bar.vtSymbol[:3] + "_FUTURE", 0)  # 暂时这么写 最好拉到外面取配置
            if self.totalCapital != 0:
                success = True
                self.lot = max(int(self.totalCapital * self.posWeight * self.pctPerTrade * bar.close / self.palValue), 1)  # 实盘单笔下单合约张数=下单币数*币价/合约面值
            else:
                success = False
                self.writeCtaLog("未获取到当前账户资金。")
        else:
            self.lot = self.totalCapital * self.posWeight * self.pctPerTrade
            success = True
        return success
    
    # 实盘在5sBar中洗价,并且在5sBar下执行出场
    def on5sBar(self, bar):
        engineType = self.getEngineType()  # 判断engine模式
        if engineType != 'backtesting':
            self.checkOnPeriodStart(bar)
            self.checkOnPeriodEnd(bar)
            for idSet in self.orderDict.values():
                self.delOrderID(idSet)
            # 执行策略逻辑
            success = self.calCurrentLot(bar)
            if success:
                # 执行策略逻辑
                self.strategy(bar)

    def onBar(self, bar):
        # 必须继承父类方法
        super().onBar(bar)
        # on bar下触发回测洗价逻辑
        engineType = self.getEngineType()  # 判断engine模式
        if engineType == 'backtesting':
            # 定时控制，开始
            self.checkOnPeriodStart(bar)
            # 定时清除已出场的单
            self.checkOnPeriodEnd(bar)
            for idSet in self.orderDict.values():
                self.delOrderID(idSet)
            # 执行策略逻辑
            success = self.calCurrentLot(bar)
            if success:
                self.strategy(bar)

    def on15MinBar(self, bar):
        engineType = self.getEngineType()  # 判断engine模式
        if engineType != 'backtesting':
            longVolume = self.getHoldVolume(self.orderDict['orderLongSet1'])
            shortVolume = self.getHoldVolume(self.orderDict['orderShortSet1'])
            self.writeCtaLog('globalStatus%s'%(self.globalStatus))
            self.writeCtaLog('longVolume:%s, shortVolume:%s'%(longVolume, shortVolume))
            self.notifyPosition('longVolume', longVolume, self.author)
            self.notifyPosition('shortVolume', shortVolume, self.author)

    def strategy(self, bar):
        signalPeriod= self.timeframeMap["signalPeriod"]
        volPeriod= self.timeframeMap["volPeriod"]
        tradePeriod= self.timeframeMap["tradePeriod"]
                
        # 根据出场信号出场
        exitLong, exitShort = self.exitSignal(signalPeriod)
        self.exitOrder(bar, exitLong, exitShort)
        
        # 根据进场信号进场
        entrySig = self.entrySignal(signalPeriod, volPeriod, tradePeriod)
        self.entryOrder(bar, entrySig)

    def exitSignal(self, signalPeriod):
        exitLong, exitShort = 0, 0
        arrayPrepared, amSignal = self.arrayPrepared(signalPeriod)
        if arrayPrepared:
            exitLong, exitShort = self.algorithm.exitSignal(amSignal, self.paraDict)
        return exitLong, exitShort

    def exitOrder(self, bar, exitLong, exitShort):
        if exitLong:
            for orderID in (self.orderDict['orderLongSet1']):
                op = self._orderPacks[orderID]
                self.composoryClose(op)
        if exitShort:
            for orderID in (self.orderDict['orderShortSet1']):
                op = self._orderPacks[orderID]
                self.composoryClose(op)

    def entrySignal(self,signalPeriod, volPeriod, tradePeriod):
        entrySig = 0
        arrayPrepared1, amSignal = self.arrayPrepared(signalPeriod)
        arrayPrepared2, amVol = self.arrayPrepared(volPeriod)


        arrayPrepared = arrayPrepared1 and arrayPrepared2
        if arrayPrepared:
            momDirection, macd, macdSig = self.algorithm.macdStatus(amSignal, self.paraDict)
            cciSig, cci = self.algorithm.cciEntrySignal(amSignal, self.paraDict)
            filterCanTrade, minusPos = self.algorithm.fliterVol(amVol, self.paraDict)
            lotMultiplier = self.algorithm.bigVolWeight(amVol, self.paraDict)
            self.globalStatus['momDirection'] = momDirection
            self.globalStatus['cciSig'] = cciSig
            self.globalStatus['minusPos'] = minusPos

            self.chartLog['datetime'].append(datetime.strptime(amSignal.datetime[-1], "%Y%m%d %H:%M:%S"))
            self.chartLog['macd'].append(macd[-1])
            self.chartLog['macdSig'].append(macdSig[-1])
            self.chartLog['cci'].append(cci[-1])

            self.lotMultipler = minusPos*lotMultiplier
            if filterCanTrade:
                if momDirection==1 and cciSig==1:
                    entrySig = 1
                elif momDirection==-1 and cciSig==-1:
                    entrySig = -1
        return entrySig

    def entryOrder(self, bar, entrySig):
        buyExecute, shortExecute = self.priceExecute(bar)
        lotSize = int(self.lot * self.lotMultipler)
        orderPos = int(lotSize//self.orderTime)
        if entrySig ==1:
            if not (self.orderDict['orderLongSet1']):
                # 如果回测直接下单，如果实盘就分批下单
                    # for orderID in self.timeLimitOrder(ctaBase.CTAORDER_BUY, self.symbol, buyExecute, self.lot, 120).vtOrderIDs:
                stepOrder1 = self.makeStepOrder(ctaBase.CTAORDER_BUY, bar.vtSymbol, buyExecute, max(lotSize, 1), max(orderPos,1), self.totalSecond, self.stepSecond)                 
                orderID1 = stepOrder1.parentID
                self.orderDict['orderLongSet1'].add(orderID1)
                self.orderLastList.append(orderID1)
                op = self._orderPacks[orderID1]
                self.setAutoExit(op,bar.close*(1-self.stoplossPct), None)

        elif entrySig ==-1:
            if not (self.orderDict['orderShortSet1']):
                    # for orderID in self.timeLimitOrder(ctaBase.CTAORDER_SHORT, self.symbol, shortExecute, self.lot, 120).vtOrderIDs:
                # 第一单
                stepOrder1 = self.makeStepOrder(ctaBase.CTAORDER_SHORT, bar.vtSymbol, shortExecute, max(lotSize, 1), max(orderPos,1), self.totalSecond, self.stepSecond)
                orderID1 = stepOrder1.parentID                
                self.orderDict['orderShortSet1'].add(orderID1)
                self.orderLastList.append(orderID1)
                op = self._orderPacks[orderID1]
                self.setAutoExit(op, bar.close*(1+self.stoplossPct), None)

 # ----------------------------------------------------------------------
    def onOrder(self, order):
        super().onOrder(order)
        pass

    # ----------------------------------------------------------------------
    # 成交后用成交价设置第一张止损止盈
    def onTrade(self, trade):
        pass

    def onStopOrder(self, so):
        pass