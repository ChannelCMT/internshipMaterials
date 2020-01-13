from vnpy.trader.vtConstant import *
import numpy as np
import talib as ta
from datetime import timedelta, datetime, time
from vnpy.trader.utils.templates.orderTemplate import * 
from vnpy.trader.app.ctaStrategy import ctaBase
from rBreakClass import rBreakSignal

########################################################################
class rBreakStrategy(OrderTemplate):
    className = 'rBreakStrategy'
    author = 'ChannelCMT'

    # 参数列表，保存了参数的名称
    paramList = [
                 # 时间周期
                 'timeframeMap',
                 #  总秒，间隔，下单次数
                 'totalSecond', 'stepSecond','orderTime',
                 # 分批进场手数
                 'lot',
                 # 品种列表
                 'symbolList',
                 # signalParameter 计算信号的参数
                 'observedPct','reversedPct', 'breakPct',
                 'rangePeriod', 'sigPeriod',
                 'trailingPct','tpTime'
                 # 低波动率过滤阈值
                #  'volPeriod', 'lowVolThreshold',
                 ]

    # 变量列表，保存了变量的名称
    varList = []

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['posDict', 'eveningDict']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        super().__init__(ctaEngine, setting)

        self.paraDict = setting
        self.barPeriod = 200
        self.symbol = self.symbolList[0]
        self.lastBarTimeDict = {frameStr: datetime(2010,1,1) for frameStr in list(set(self.timeframeMap.values()))}
        self.algorithm = rBreakSignal()
        self.observedLong, self.observedShort, self.reversedLong, self.reversedShort, self.breakLong, self.breakShort = np.array([]),np.array([]),np.array([]),np.array([]),np.array([]),np.array([])
        # varialbes
        self.orderDict = {'orderLongSet':set(), 'orderShortSet': set()}
        self.lastOrderDict = {'nextExecuteTime': datetime(2000, 1, 1)}
        
        # 打印全局信号的字典
        self.globalStatus = {}
        self.chartLog = {
                        'datetime':[],
                        'observedLong': [],
                        'observedShort': [],
                        'reversedLong':[],
                        'reversedShort':[],
                        'breakLong':[],
                        'breakShort':[]
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

    def calIndicator(self, bar):
        if bar.datetime.time()== time(10,00):
            return True
        else:
            return False

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
                # self.lastOrderDict['nextExecuteTime'] = self.currentTime + timedelta(hours=self.stopControlTime)
    
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
            closedVolume = self.orderClosedVolume(op)
            pos+= (holdVolume-closedVolume)
        return pos
    
    # 实盘在5sBar中洗价
    def on5sBar(self, bar):
        engineType = self.getEngineType()  # 判断engine模式
        if engineType != 'backtesting':
            self.checkOnPeriodStart(bar)
            self.checkOnPeriodEnd(bar)
            for idSet in self.orderDict.values():
                self.delOrderID(idSet)

    def onBar(self, bar):
        # 必须继承父类方法
        super().onBar(bar)
        # on bar下触发回测洗价逻辑
        engineType = self.getEngineType()  # 判断engine模式
        if engineType == 'backtesting':
            # 定时控制，开始
            self.checkOnPeriodStart(bar)
            # 回测时的下单手数按此方法调整
            self.lot = int(200000 / bar.close)
        # 定时清除已出场的单
            self.checkOnPeriodStart(bar)
            self.checkOnPeriodEnd(bar)
            for idSet in self.orderDict.values():
                self.delOrderID(idSet)
            # 执行策略逻辑
            self.strategy(bar)

    def on5MinBar(self, bar):
        engineType = self.getEngineType()  # 判断engine模式
        if engineType != 'backtesting':
            self.writeCtaLog('globalStatus%s'%(self.globalStatus))
            self.writeCtaLog('firstVolume:%s, secondVolume:%s'%(self.getHoldVolume(self.orderDict['orderLongSet']), self.getHoldVolume(self.orderDict['orderShortSet'])))
        else:
            pass
    def strategy(self, bar):
        signalPeriod= self.timeframeMap["signalPeriod"]

        # 根据出场信号出场
        exitLong, exitShort = self.exitSignal(signalPeriod)
        self.exitOrder(exitLong, exitShort)

        # 根据进场信号进场
        longSignal, shortSignal = self.entrySignal(bar, signalPeriod)
        self.entryOrder(bar, longSignal, shortSignal)

    def exitSignal(self, signalPeriod):

        exitLong, exitShort = 0,0
        arrayPrepared, amSignal = self.arrayPrepared(signalPeriod)
        if arrayPrepared:
            if len(self.observedLong):
                if amSignal.close[-1]<self.observedLong[-1]:
                    exitLong = 1
                if amSignal.close[-1]>self.observedShort[-1]:
                    exitShort = 1
        return exitLong, exitShort

    def exitOrder(self, exitLong, exitShort):
        if exitLong == 1:
            for orderID in (self.orderDict['orderLongSet']):
                op = self._orderPacks[orderID]
                self.composoryClose(op)
        if exitShort==1:
            for orderID in (self.orderDict['orderShortSet']):
                op = self._orderPacks[orderID]
                self.composoryClose(op)

    def entrySignal(self, bar, signalPeriod):
        longSignal, shortSignal = 0, 0
        arrayPrepared, amSignal = self.arrayPrepared(signalPeriod)
        # amSignalDatetime = datetime.strptime(amSignal.datetime[-1], "%Y%m%d %H:%M:%S")
        if arrayPrepared:
            longSignal, shortSignal = 0, 0
            if self.calIndicator(bar):
                self.observedLong, self.observedShort, self.reversedLong, self.reversedShort, self.breakLong, self.breakShort = self.algorithm.rBreak(amSignal, self.paraDict)
            # upRevertDn, dnRevertUp, upBreak, dnBreak
            if len(self.observedLong):
                upRevertDn = ta.MAX(amSignal.high, self.sigPeriod)[-1]>ta.MAX(self.observedShort, self.sigPeriod)[-1] and amSignal.close[-1]<self.reversedShort[-1] and amSignal.close[-2]>=self.reversedShort[-1]
                dnRevertUp = ta.MIN(amSignal.low, self.sigPeriod)[-1]<ta.MIN(self.observedLong, self.sigPeriod)[-1] and amSignal.close[-1]>self.reversedLong[-1] and amSignal.close[-2]<=self.reversedLong[-1]
                upBreak = amSignal.high[-1]>self.breakLong[-1] and amSignal.high[-2]<=self.breakLong[-1]
                dnBreak = amSignal.low[-1]<self.breakShort[-1] and amSignal.low[-2]>=self.breakShort[-1]
                
                if dnRevertUp or upBreak:
                    longSignal = 1
                if upRevertDn or dnBreak:
                    shortSignal = 1
                self.globalStatus['longSignal'] = longSignal
                self.globalStatus['shortSignal'] = shortSignal

                self.chartLog['datetime'].append(datetime.strptime(amSignal.datetime[-1], "%Y%m%d %H:%M:%S"))
                self.chartLog['observedLong'].append(self.observedLong[-1])
                self.chartLog['observedShort'].append(self.observedShort[-1])
                self.chartLog['reversedLong'].append(self.reversedLong[-1])
                self.chartLog['reversedShort'].append(self.reversedShort[-1])
                self.chartLog['breakLong'].append(self.breakLong[-1])
                self.chartLog['breakShort'].append(self.breakShort[-1])
        return longSignal, shortSignal

    def entryOrder(self, bar, longSignal, shortSignal):
        buyExecute, shortExecute = self.priceExecute(bar)
        if longSignal ==1:
            if not self.orderDict['orderLongSet']:
                # 如果回测直接下单，如果实盘就分批下单
                longPos = self.lot//self.orderTime
                    # for orderID in self.timeLimitOrder(ctaBase.CTAORDER_BUY, self.symbol, buyExecute, self.lot, 120).vtOrderIDs:
                stepOrder = self.makeStepOrder(ctaBase.CTAORDER_BUY, bar.vtSymbol, buyExecute, self.lot, longPos, self.totalSecond, self.stepSecond)
                orderID = stepOrder.parentID
                self.orderDict['orderLongSet'].add(orderID)
                # op = self._orderPacks[orderID]
                # self.setAutoExit(op, bar.close*(1-self.trailingPct),  bar.close*(1+self.tpTime*self.trailingPct))

        elif shortSignal ==1:
            if not self.orderDict['orderShortSet']:
                shortPos = self.lot//self.orderTime
                stepOrder = self.makeStepOrder(ctaBase.CTAORDER_SHORT, bar.vtSymbol, shortExecute, self.lot, shortPos, self.totalSecond, self.stepSecond)
                orderID = stepOrder.parentID                
                self.orderDict['orderShortSet'].add(orderID)
                # op = self._orderPacks[orderID]
                # self.setAutoExit(op, bar.close*(1+self.trailingPct),  bar.close*(1-self.tpTime*self.trailingPct))
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