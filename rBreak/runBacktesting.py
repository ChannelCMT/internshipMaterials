"""
展示如何执行策略回测。
"""
from vnpy.trader.app.ctaStrategy import BacktestingEngine
import pandas as pd
from vnpy.trader.utils import htmlplot
import json
from datetime import datetime

if __name__ == '__main__':
    from rBreakStrategy import rBreakStrategy
    # 创建回测引擎
    engine = BacktestingEngine()
    # engine.setDB_URI("mongodb://172.16.11.81:27017")
    engine.setDB_URI("mongodb://localhost:27017")


    # Bar回测
    engine.setBacktestingMode(engine.BAR_MODE)
    engine.setDatabase('Kline_1Min_Auto_Db_Plus')

    # Tick回测
    # engine.setBacktestingMode(engine.TICK_MODE)
    # engine.setDatabase('VnTrader_1Min_Db', 'VnTrader_Tick_Db')

    # 设置回测用的数据起始日期，initHours 默认值为 0
    engine.setDataRange(datetime(2018,6,1), datetime(2020,2,10), datetime(2018,5,1))


    # 设置产品相关参数
    engine.setCapital(1000)  # 设置起始资金，默认值是1,000,000
    contracts = [{
                    "symbol":"btc_usd_cq.future:okex",
                    "size" : 1, # 每点价值
                    "priceTick" : 0.001, # 最小价格变动
                    "rate" : 5/10000, # 单边手续费
                    "slippage" : 0.005 # 滑价
                    },] 
    engine.setContracts(contracts)
    engine.setLog(True, "./log") 
    
    with open("CTA_setting.json") as parameterDict:
        setting = json.load(parameterDict)
    
    engine.initStrategy(rBreakStrategy, setting[0])
    
    # 开始跑回测
    engine.runBacktesting()
    
    # 显示回测结果
    engine.showBacktestingResult()
    engine.showDailyResult()
    
    ### 画图分析
    chartLog = pd.DataFrame(engine.strategy.chartLog).set_index('datetime')
    # print(chartLog)
    mp = htmlplot.getXMultiPlot(engine, freq="60m")
    mp.addLine(line=chartLog[['observedLong', 'observedShort', 
        'reversedLong', 'reversedShort', 'breakLong', 'breakShort'
        ]].reset_index(), pos=0)
    mp.resample()
    mp.show()
 